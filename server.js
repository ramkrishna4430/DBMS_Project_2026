require('dotenv').config();
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const bcrypt = require('bcrypt');
const oracledb = require('oracledb');

const app = express();
const port = 3000;

app.use(cors());
app.use(bodyParser.json());
app.use(express.static('.'));

// --- DATABASE CONNECTION ---
// Configure outFormat so that query results are returned as objects instead of arrays
oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
// Avoid blocking on startup if the DB isn't immediately available
oracledb.autoCommit = true;

let pool;

async function initDb() {
    try {
        pool = await oracledb.createPool({
            user: 'C##rk_4430',
            password: process.env.DB_PASSWORD,
            connectString: 'localhost:1521/XE', // Adjust 'XE' if your Oracle SID/Service is different
            poolMin: 2,
            poolMax: 10,
            poolIncrement: 2
        });
        console.log('Connected to Oracle Database at localhost:1521/XE');

        // Test connection
        const result = await executeQuery("SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS now FROM DUAL");
        console.log('Database Time:', result.rows[0].now);
    } catch (err) {
        console.error('Connection Error:', err);
    }
}
initDb();

// Helper to lowercase keys of objects returned by Oracle, because the frontend expects lower case
// like `user_id`, `full_name`, etc. instead of `USER_ID`, `FULL_NAME`.
function lowerCaseKeys(obj) {
    if (obj === null || obj === undefined) return obj;
    if (Array.isArray(obj)) return obj.map(lowerCaseKeys);
    if (typeof obj !== 'object' || obj instanceof Date) return obj;

    const newObj = {};
    for (let key in obj) {
        if (Object.prototype.hasOwnProperty.call(obj, key)) {
            newObj[key.toLowerCase()] = obj[key];
        }
    }
    return newObj;
}

// Global query wrapper to map query calls easily, mimic pg pool.query behavior
const executeQuery = async (sql, binds = [], options = {}) => {
    let connection;
    try {
        connection = await pool.getConnection();
        const executeOptions = { autoCommit: true, ...options };
        const result = await connection.execute(sql, binds, executeOptions);
        if (result.rows) {
            result.rows = lowerCaseKeys(result.rows);
        }
        return result;
    } finally {
        if (connection) {
            try { await connection.close(); } catch (err) { }
        }
    }
};

// ==========================================
// 1. AUTHENTICATION ROUTES
// ==========================================

app.post('/api/signup', async (req, res) => {
    const { full_name, email, password, phone, role } = req.body;
    let client;
    try {
        client = await pool.getConnection();
        const hash = await bcrypt.hash(password, 10);

        const userRes = await client.execute(
            'INSERT INTO users (full_name, email, password_hash, phone, role) VALUES (:1, :2, :3, :4, :5) RETURNING user_id INTO :6',
            [full_name, email, hash, phone, role, { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }]
        );
        const uid = userRes.outBinds[0][0];

        let roleId;
        if (role === 'customer') {
            const r = await client.execute('INSERT INTO customer (user_id) VALUES (:1) RETURNING customer_id INTO :2', [uid, { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }]);
            roleId = r.outBinds[0][0];
        } else {
            const r = await client.execute('INSERT INTO distributor (user_id, company_name) VALUES (:1, :2) RETURNING distributor_id INTO :3', [uid, full_name, { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }]);
            roleId = r.outBinds[0][0];
        }
        await client.commit();
        res.json({ user_id: uid, role_id: roleId, full_name, email, role });
    } catch (e) {
        if (client) await client.rollback();
        res.status(500).json({ error: e.message });
    } finally {
        if (client) { try { await client.close(); } catch (e) { } }
    }
});

app.post('/api/login', async (req, res) => {
    const { email, password } = req.body;
    try {
        const u = await executeQuery('SELECT * FROM users WHERE email = :1', [email]);
        if (u.rows.length === 0) return res.status(401).json({ error: 'User not found' });
        const user = u.rows[0];
        if (await bcrypt.compare(password, user.password_hash)) {
            let roleId;
            if (user.role === 'customer') {
                const r = await executeQuery('SELECT customer_id FROM customer WHERE user_id = :1', [user.user_id]);
                roleId = r.rows[0].customer_id;
            } else {
                const r = await executeQuery('SELECT distributor_id FROM distributor WHERE user_id = :1', [user.user_id]);
                roleId = r.rows[0].distributor_id;
            }
            res.json({ user_id: user.user_id, role_id: roleId, full_name: user.full_name, role: user.role, email: user.email });
        } else { res.status(401).json({ error: 'Wrong password' }); }
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// ==========================================
// 2. PRODUCT & INVENTORY ROUTES
// ==========================================

// Get all products (For Customer Marketplace)
app.get('/api/products', async (req, res) => {
    try {
        const r = await executeQuery(`
            SELECT p.*, i.stock_quantity, get_avg_rating(p.product_id) as avg_rating 
            FROM products p JOIN inventory i ON p.product_id = i.product_id`);
        res.json(r.rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Add a new product (For Distributor Portal)
app.post('/api/products', async (req, res) => {
    const { product_name, category, price, stock_quantity, min_stock, distributor_id } = req.body;
    let client;
    try {
        client = await pool.getConnection();
        const productRes = await client.execute(
            'INSERT INTO products (product_name, category, price, distributor_id) VALUES (:1, :2, :3, :4) RETURNING product_id INTO :5',
            [product_name, category, price, distributor_id, { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }]
        );
        const productId = productRes.outBinds[0][0];
        await client.execute(
            'INSERT INTO inventory (product_id, stock_quantity, min_stock) VALUES (:1, :2, :3)',
            [productId, stock_quantity, min_stock]
        );
        await client.commit();
        res.status(201).json({ success: true, message: 'Product added successfully' });
    } catch (error) {
        if (client) await client.rollback();
        res.status(500).json({ error: error.message });
    } finally {
        if (client) { try { await client.close(); } catch (e) { } }
    }
});

// Update a product (For Distributor Edit Modal)
app.put('/api/products/:productId', async (req, res) => {
    const { productId } = req.params;
    const { product_name, category, price, stock_quantity, min_stock } = req.body;
    let client;
    try {
        client = await pool.getConnection();
        await client.execute(
            'UPDATE products SET product_name = :1, category = :2, price = :3 WHERE product_id = :4',
            [product_name, category, price, productId]
        );
        await client.execute(
            'UPDATE inventory SET stock_quantity = :1, min_stock = :2 WHERE product_id = :3',
            [stock_quantity, min_stock, productId]
        );
        await client.commit();
        res.json({ success: true, message: 'Product updated successfully' });
    } catch (error) {
        if (client) await client.rollback();
        res.status(400).json({ error: error.message });
    } finally {
        if (client) { try { await client.close(); } catch (e) { } }
    }
});

// Delete a product (For Distributor)
app.delete('/api/products/:productId', async (req, res) => {
    const { productId } = req.params;
    try {
        await executeQuery('DELETE FROM products WHERE product_id = :1', [productId]);
        res.json({ success: true, message: 'Product deleted successfully' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==========================================
// 3. CART ROUTES (Customer)
// ==========================================

// Add item to cart (upsert - if item exists, increment quantity)
app.post('/api/cart', async (req, res) => {
    const { customer_id, product_id, quantity } = req.body;
    try {
        await executeQuery(`
            MERGE INTO cart c
            USING DUAL ON (c.customer_id = :cid AND c.product_id = :pid)
            WHEN MATCHED THEN
                UPDATE SET c.quantity = c.quantity + :qty
            WHEN NOT MATCHED THEN
                INSERT (customer_id, product_id, quantity)
                VALUES (:cid, :pid, :qty)
        `, { cid: customer_id, pid: product_id, qty: quantity });
        res.json({ success: true, message: 'Added to cart' });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Get cart items for a customer
app.get('/api/cart/:customerId', async (req, res) => {
    try {
        const r = await executeQuery(`
            SELECT c.cart_id, c.product_id, c.quantity, p.product_name, p.price
            FROM cart c
            JOIN products p ON c.product_id = p.product_id
            WHERE c.customer_id = :1
        `, [req.params.customerId]);
        res.json(r.rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Update cart item quantity (delete if quantity <= 0)
app.put('/api/cart', async (req, res) => {
    const { customer_id, product_id, quantity } = req.body;
    try {
        if (quantity <= 0) {
            await executeQuery(
                'DELETE FROM cart WHERE customer_id = :1 AND product_id = :2',
                [customer_id, product_id]
            );
        } else {
            await executeQuery(
                'UPDATE cart SET quantity = :1 WHERE customer_id = :2 AND product_id = :3',
                [quantity, customer_id, product_id]
            );
        }
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Get cart item count (for badge)
app.get('/api/cart-count/:customerId', async (req, res) => {
    try {
        const r = await executeQuery(
            'SELECT COALESCE(SUM(quantity), 0) as count FROM cart WHERE customer_id = :1',
            [req.params.customerId]
        );
        res.json({ count: parseInt(r.rows[0].count) });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ==========================================
// 4. ORDER PROCESSING
// ==========================================

app.post('/api/process-order/:customerId', async (req, res) => {
    try {
        await executeQuery('BEGIN process_order(:1); END;', [req.params.customerId]);
        // Get the latest order for this customer (just created by the procedure)
        const orderRes = await executeQuery(
            'SELECT order_id, total_amount FROM orders WHERE customer_id = :1 ORDER BY order_date DESC FETCH FIRST 1 ROWS ONLY',
            [req.params.customerId]
        );
        const order = orderRes.rows[0];
        res.json({ message: 'Order Successful', order_id: order.order_id, total_amount: order.total_amount });
    } catch (e) {
        res.status(400).json({ error: e.message });
    }
});

// Get customer orders with items
app.get('/api/orders/:customerId', async (req, res) => {
    try {
        const ordersRes = await executeQuery(`
            SELECT o.order_id, o.order_date, o.status, o.total_amount
            FROM orders o
            WHERE o.customer_id = :1
            ORDER BY o.order_date DESC
        `, [req.params.customerId]);

        const orders = [];
        for (const order of ordersRes.rows) {
            const itemsRes = await executeQuery(`
                SELECT oi.product_id, oi.quantity, oi.price, p.product_name
                FROM order_item oi
                JOIN products p ON oi.product_id = p.product_id
                WHERE oi.order_id = :1
            `, [order.order_id]);
            orders.push({ ...order, items: itemsRes.rows });
        }
        res.json(orders);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ==========================================
// 5. REVIEW ROUTES
// ==========================================

app.post('/api/review', async (req, res) => {
    const { customer_id, product_id, rating, comment } = req.body;
    try {
        await executeQuery(`
            INSERT INTO review (customer_id, product_id, rating, review_text)
            VALUES (:1, :2, :3, :4)
        `, [customer_id, product_id, rating, comment]);
        res.json({ success: true, message: 'Review submitted' });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ==========================================
// 6. LOYALTY DISCOUNT (Function call)
// ==========================================

app.get('/api/loyalty-discount/:customerId', async (req, res) => {
    try {
        const r = await executeQuery(
            'SELECT calculate_loyalty_discount(:1) as discount FROM DUAL',
            [req.params.customerId]
        );
        res.json({ discount: parseFloat(r.rows[0].discount) });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ==========================================
// 7. ACCOUNT MANAGEMENT ROUTES
// ==========================================

app.get('/api/account/:userId', async (req, res) => {
    try {
        const { userId } = req.params;
        const result = await executeQuery(`
            SELECT u.email, u.full_name, u.phone, u.role,
                   c.address, c.city, c.state, c.pincode,
                   d.company_name, d.warehouse_location
            FROM users u
            LEFT JOIN customer c ON u.user_id = c.user_id
            LEFT JOIN distributor d ON u.user_id = d.user_id
            WHERE u.user_id = :1
        `, [userId]);

        if (result.rows.length === 0) return res.status(404).json({ error: 'User not found' });
        res.json(result.rows[0]);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.put('/api/account/:userId', async (req, res) => {
    const { userId } = req.params;
    const { full_name, phone, company_name, warehouse_location, address, city, state, pincode } = req.body;
    let client;

    try {
        client = await pool.getConnection();
        const userRes = await client.execute(
            'UPDATE users SET full_name = :1, phone = :2 WHERE user_id = :3 RETURNING role INTO :4',
            [full_name, phone, userId, { dir: oracledb.BIND_OUT, type: oracledb.STRING }]
        );

        if (userRes.outBinds[0] && userRes.outBinds[0].length > 0) {
            const role = userRes.outBinds[0][0];
            if (role === 'distributor') {
                await client.execute(
                    'UPDATE distributor SET company_name = :1, warehouse_location = :2 WHERE user_id = :3',
                    [company_name, warehouse_location, userId]
                );
            } else if (role === 'customer') {
                await client.execute(
                    'UPDATE customer SET address = :1, city = :2, state = :3, pincode = :4 WHERE user_id = :5',
                    [address, city, state, pincode, userId]
                );
            }
        }

        await client.commit();
        res.json({ success: true, message: 'Account updated successfully' });
    } catch (error) {
        if (client) await client.rollback();
        res.status(500).json({ error: error.message });
    } finally {
        if (client) { try { await client.close(); } catch (e) { } }
    }
});

// ==========================================
// 8. DISTRIBUTOR DASHBOARD ROUTES
// ==========================================

// Dashboard stats
app.get('/api/distributor/dashboard/:distributorId', async (req, res) => {
    const { distributorId } = req.params;
    try {
        const prodCount = await executeQuery(
            'SELECT COUNT(*) as count FROM products WHERE distributor_id = :1',
            [distributorId]
        );

        const revenueRes = await executeQuery(`
            SELECT COALESCE(SUM(oi.price * oi.quantity), 0) as total
            FROM order_item oi
            JOIN products p ON oi.product_id = p.product_id
            JOIN orders o ON oi.order_id = o.order_id
            WHERE p.distributor_id = :1 AND o.status = 'delivered'
        `, [distributorId]);

        const pendingRes = await executeQuery(`
            SELECT COUNT(DISTINCT o.order_id) as count
            FROM orders o
            JOIN order_item oi ON o.order_id = oi.order_id
            JOIN products p ON oi.product_id = p.product_id
            WHERE p.distributor_id = :1 AND o.status = 'pending'
        `, [distributorId]);

        const lowStockRes = await executeQuery(`
            SELECT COUNT(*) as count
            FROM inventory i
            JOIN products p ON i.product_id = p.product_id
            WHERE p.distributor_id = :1 AND i.stock_quantity <= i.min_stock
        `, [distributorId]);

        res.json({
            total_products: parseInt(prodCount.rows[0].count),
            total_revenue: parseFloat(revenueRes.rows[0].total),
            pending_orders: parseInt(pendingRes.rows[0].count),
            low_stock_alerts: parseInt(lowStockRes.rows[0].count)
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Get distributor's products with inventory
app.get('/api/distributor/products/:distributorId', async (req, res) => {
    try {
        const r = await executeQuery(`
            SELECT p.product_id, p.product_name, p.category, p.price,
                   i.stock_quantity, i.min_stock
            FROM products p
            LEFT JOIN inventory i ON p.product_id = i.product_id
            WHERE p.distributor_id = :1
            ORDER BY p.product_id
        `, [req.params.distributorId]);
        res.json(r.rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Get orders containing this distributor's products
app.get('/api/distributor/orders/:distributorId', async (req, res) => {
    try {
        const orderIdsRes = await executeQuery(`
            SELECT DISTINCT o.order_id, o.order_date, o.status, o.total_amount,
                   u.full_name as customer_name
            FROM orders o
            JOIN order_item oi ON o.order_id = oi.order_id
            JOIN products p ON oi.product_id = p.product_id
            JOIN customer c ON o.customer_id = c.customer_id
            JOIN users u ON c.user_id = u.user_id
            WHERE p.distributor_id = :1
            ORDER BY o.order_date DESC
        `, [req.params.distributorId]);

        const orders = [];
        for (const order of orderIdsRes.rows) {
            const itemsRes = await executeQuery(`
                SELECT oi.product_id, oi.quantity, oi.price, p.product_name
                FROM order_item oi
                JOIN products p ON oi.product_id = p.product_id
                WHERE oi.order_id = :1 AND p.distributor_id = :2
            `, [order.order_id, req.params.distributorId]);

            const payRes = await executeQuery(
                'SELECT payment_status FROM payment WHERE order_id = :1',
                [order.order_id]
            );

            orders.push({
                ...order,
                items: itemsRes.rows,
                payment_status: payRes.rows.length > 0 ? payRes.rows[0].payment_status : 'pending'
            });
        }
        res.json(orders);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ==========================================
// 9. PAYMENT ROUTES
// ==========================================

app.post('/api/payment/:orderId', async (req, res) => {
    const { orderId } = req.params;
    const { payment_method, payment_status } = req.body;
    try {
        const existing = await executeQuery('SELECT payment_id FROM payment WHERE order_id = :1', [orderId]);

        if (existing.rows.length > 0) {
            await executeQuery(
                'UPDATE payment SET payment_status = :1, payment_method = :2 WHERE order_id = :3',
                [payment_status, payment_method, orderId]
            );
        } else {
            const orderRes = await executeQuery('SELECT total_amount FROM orders WHERE order_id = :1', [orderId]);
            const amount = orderRes.rows[0].total_amount;

            await executeQuery(
                "INSERT INTO payment (order_id, payment_method, payment_status, amount) VALUES (:1, :2, 'pending', :3)",
                [orderId, payment_method, amount]
            );

            if (payment_status === 'completed') {
                await executeQuery(
                    "UPDATE payment SET payment_status = 'completed' WHERE order_id = :1",
                    [orderId]
                );
            }
        }

        res.json({ success: true, message: 'Payment processed' });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ==========================================
// 10. ORDER STATUS UPDATE (Distributor)
// ==========================================

app.put('/api/orders/:orderId/status', async (req, res) => {
    const { orderId } = req.params;
    const { status } = req.body;
    try {
        await executeQuery('UPDATE orders SET status = :1 WHERE order_id = :2', [status, orderId]);
        res.json({ success: true, message: 'Order status updated to ' + status });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ==========================================
// 11. SALES REPORT (Procedure call)
// ==========================================

app.post('/api/sales-report', async (req, res) => {
    const { start_date, end_date } = req.body;
    try {
        const sd = new Date(start_date);
        const ed = new Date(end_date);
        const r = await executeQuery(
            `BEGIN generate_sales_report(:sd, :ed, :p_total_revenue, :p_top_product_name, :p_top_qty_sold); END;`,
            {
                sd,
                ed,
                p_total_revenue: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER },
                p_top_product_name: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
                p_top_qty_sold: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
            }
        );
        res.json({
            total_revenue: parseFloat(r.outBinds.p_total_revenue) || 0,
            top_product_name: r.outBinds.p_top_product_name || 'No sales in this period',
            top_qty_sold: parseInt(r.outBinds.p_top_qty_sold) || 0
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// --- START SERVER ---
app.listen(port, () => console.log(`Server running at http://localhost:${port}`));