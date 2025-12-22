CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(50),
    in_stock BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_id INT REFERENCES products(id),
    quantity INT DEFAULT 1,
    status VARCHAR(20) DEFAULT 'pending',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users (username, email, age) VALUES
    ('alice', 'alice@example.com', 25),
    ('bob', 'bob@example.com', 30),
    ('charlie', 'charlie@example.com', 35)
ON CONFLICT (username) DO NOTHING;

INSERT INTO products (name, price, category) VALUES
    ('Laptop', 999.99, 'Electronics'),
    ('Book', 19.99, 'Education'),
    ('Coffee Mug', 12.50, 'Home')
ON CONFLICT DO NOTHING;