--FUNCTIONS | PROCEDURE | TIGGER -- 


CREATE OR REPLACE FUNCTION get_avg_rating(p_product_id IN NUMBER)
RETURN NUMBER IS
    v_avg NUMBER(3,1);
BEGIN
    -- Calculate average rating, return 0 if there are no reviews
    -- Using NVL instead of COALESCE for simple null replacement
    SELECT NVL(ROUND(AVG(rating), 1), 0.0) INTO v_avg
    FROM review
    WHERE product_id = p_product_id;

    RETURN v_avg;
END;
/

-------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE process_order(p_customer_id IN NUMBER) IS
    v_order_id NUMBER;
    v_total_amount NUMBER(10,2) := 0;
    v_price NUMBER(10,2);
BEGIN
    -- 1. Calculate the total order amount from the cart
    SELECT NVL(SUM(p.price * c.quantity), 0) INTO v_total_amount
    FROM cart c
    JOIN products p ON c.product_id = p.product_id
    WHERE c.customer_id = p_customer_id;

    IF v_total_amount = 0 THEN
        -- Oracle uses RAISE_APPLICATION_ERROR with custom codes between -20000 and -20999
        RAISE_APPLICATION_ERROR(-20001, 'Cannot place order: Cart is empty.');
    END IF;

    -- 2. Insert the new order and get the generated order_id
    INSERT INTO orders (customer_id, status, total_amount)
    VALUES (p_customer_id, 'pending', v_total_amount)
    RETURNING order_id INTO v_order_id;

    -- 3. Loop through cart items to populate order_items and update inventory
    FOR v_cart_record IN (SELECT product_id, quantity FROM cart WHERE customer_id = p_customer_id) LOOP
        -- Get the current product price
        SELECT price INTO v_price FROM products WHERE product_id = v_cart_record.product_id;

        -- Insert into order_item
        INSERT INTO order_item (order_id, product_id, quantity, price)
        VALUES (v_order_id, v_cart_record.product_id, v_cart_record.quantity, v_price);

        -- Deduct from inventory (NOTE: This action will fire our Trigger!)
        UPDATE inventory
        SET stock_quantity = stock_quantity - v_cart_record.quantity
        WHERE product_id = v_cart_record.product_id;
    END LOOP;

    -- 4. Empty the customer's cart after successful order
    DELETE FROM cart WHERE customer_id = p_customer_id;
    
    -- Optional: Commit the transaction if you want the procedure to handle it
    -- COMMIT; 
END;
/

--------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRIGGER check_min_stock
BEFORE UPDATE ON inventory
FOR EACH ROW
BEGIN
    -- Check if the new stock quantity drops below the minimum required
    IF :NEW.stock_quantity < :NEW.min_stock THEN
        RAISE_APPLICATION_ERROR(-20002, 
            'Transaction Blocked! Cannot dispatch order. Stock quantity (' || :NEW.stock_quantity || 
            ') would fall below the required min_stock (' || :NEW.min_stock || 
            ') for Product ID ' || :NEW.product_id || '.');
    END IF;
END;
/

-------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION calculate_loyalty_discount(p_customer_id IN NUMBER)
RETURN NUMBER IS
    v_total_spent NUMBER(10,2) := 0;
    v_discount_pct NUMBER(5,2) := 0.00;
BEGIN
    -- Calculate total amount spent by the customer on delivered orders
    SELECT NVL(SUM(total_amount), 0) INTO v_total_spent
    FROM orders
    WHERE customer_id = p_customer_id AND status = 'delivered';

    -- Determine discount percentage based on lifetime spending
    IF v_total_spent >= 50000 THEN
        v_discount_pct := 10.00; -- 10% discount for spending 50k+
    ELSIF v_total_spent >= 10000 THEN
        v_discount_pct := 5.00;  -- 5% discount for spending 10k+
    ELSE
        v_discount_pct := 0.00;  -- No discount
    END IF;

    RETURN v_discount_pct;
END;
/

----------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE generate_sales_report(
    p_start_date IN TIMESTAMP,
    p_end_date IN TIMESTAMP,
    p_total_revenue OUT NUMBER,
    p_top_product_name OUT VARCHAR2,
    p_top_qty_sold OUT NUMBER
) IS
BEGIN
    -- Initialize OUT variables
    p_total_revenue := 0;
    p_top_product_name := 'No sales in this period';
    p_top_qty_sold := 0;

    -- 1. Calculate total revenue for the given period
    SELECT NVL(SUM(total_price), 0) INTO p_total_revenue
    FROM sales_history
    WHERE sale_date BETWEEN p_start_date AND p_end_date;

    -- 2. Identify the top-selling product by quantity in that period
    BEGIN
        SELECT product_name, total_qty INTO p_top_product_name, p_top_qty_sold
        FROM (
            SELECT p.product_name, SUM(sh.quantity_sold) as total_qty
            FROM sales_history sh
            JOIN products p ON sh.product_id = p.product_id
            WHERE sh.sale_date BETWEEN p_start_date AND p_end_date
            GROUP BY p.product_id, p.product_name
            ORDER BY total_qty DESC
        )
        WHERE ROWNUM = 1; -- Oracle equivalent of LIMIT 1
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- Handled natively by our initialization above, but good practice to catch
            p_top_product_name := 'No sales in this period';
            p_top_qty_sold := 0;
    END;
END;
/

-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRIGGER trigger_log_sales
AFTER UPDATE ON payment
FOR EACH ROW
DECLARE
    v_line_total NUMBER(10,2);     
BEGIN
    -- Only proceed if the payment status is newly marked as 'completed'
    -- Using NVL to handle cases where OLD.payment_status might have been NULL
    IF :NEW.payment_status = 'completed' AND NVL(:OLD.payment_status, 'x') != 'completed' THEN
        
        -- Loop through all items in the corresponding order
        FOR v_item IN (SELECT product_id, quantity, price FROM order_item WHERE order_id = :NEW.order_id) LOOP
            
            -- Calculate total price for this specific item batch
            v_line_total := v_item.quantity * v_item.price;

            -- Insert the record into sales_history
            INSERT INTO sales_history (product_id, order_id, quantity_sold, total_price)
            VALUES (v_item.product_id, :NEW.order_id, v_item.quantity, v_line_total);
            
        END LOOP;
        
    END IF;
END;
/



--------------------------------------------------------------------------------------------------------------------------------------