#!/usr/bin/env python3
"""
Simple Inventory Management API
Connects to QuestDB for time-series inventory tracking
"""

from flask import Flask, request, jsonify
import requests
import json
from datetime import datetime
import uuid #generates unique identifiers

app = Flask(__name__) #initializes web server

# QuestDB connection settings(function safely sends a SQL query to QuestDB and handles errors gracefully.)
QUESTDB_HOST = "localhost"
QUESTDB_PORT = 9000
QUESTDB_URL = f"http://{QUESTDB_HOST}:{QUESTDB_PORT}"

def execute_query(sql):
    """Execute SQL query on QuestDB"""
    try:
        response = requests.get(f"{QUESTDB_URL}/exec", params={"query": sql})
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"QuestDB error: {response.text}"}
    except Exception as e:
        return {"error": f"Connection error: {str(e)}"}

# ============================================================================
# PRODUCT MANAGEMENT APIS
# ============================================================================

@app.route('/api/products', methods=['POST']) #creates a new product record in the database.
def create_product():
    """Create a new product""" #API
    data = request.get_json()
    
    product_id = data.get('id') or f"PROD{uuid.uuid4().hex[:6].upper()}"
    name = data.get('name', '')
    description = data.get('description', '')
    category = data.get('category', '')
    unit_price = data.get('unit_price', 0.0)
    
    sql = f"""
    INSERT INTO products VALUES (
        '{product_id}', '{name}', '{description}', '{category}', 
        {unit_price}, now(), now()   ##to store current timestamps for created_at and updated_at
    )
    """
    
    result = execute_query(sql)
    if 'error' not in result:
        return jsonify({"success": True, "product_id": product_id, "message": "Product created"}), 201 #If insertion succeeds: returns a JSON response with 201 Created.
    else:
        return jsonify(result), 500 #If there’s an error (DB issue): returns error with status 500

@app.route('/api/products', methods=['GET']) #to fetch all products.
def get_products():
    """Get all products""" #API
    sql = "SELECT * FROM products ORDER BY created_at DESC"
    result = execute_query(sql) #Query to fetch all products, ordered by latest creation date.
    
    if 'error' not in result:
        return jsonify({
            "success": True,
            "products": result.get('dataset', []),
            "columns": [col['name'] for col in result.get('columns', [])]
        })
    else:
        return jsonify(result), 500

@app.route('/api/products/<product_id>', methods=['GET']) 
def get_product(product_id):
    """Get specific product""" #API
    sql = f"SELECT * FROM products WHERE id = '{product_id}'" #Fetches only the product with matching id
    result = execute_query(sql)
    
    if 'error' not in result and result.get('count', 0) > 0:
        return jsonify({
            "success": True,
            "product": result['dataset'][0],
            "columns": [col['name'] for col in result.get('columns', [])]
        })
    else:
        return jsonify({"error": "Product not found"}), 404

# ============================================================================
# STOCK MANAGEMENT APIS
# ============================================================================

@app.route('/api/stock/<product_id>', methods=['GET'])
def get_stock(product_id): #Fetches latest stock record for one product.
    """Get current stock level for a product""" #API
    sql = f"SELECT * FROM stock WHERE product_id = '{product_id}' ORDER BY last_updated DESC LIMIT 1"
    result = execute_query(sql)
    
    if 'error' not in result and result.get('count', 0) > 0:
        return jsonify({
            "success": True,
            "stock": result['dataset'][0],
            "columns": [col['name'] for col in result.get('columns', [])]
        })
    else:
        return jsonify({"error": "Stock not found", "quantity": 0}), 404

@app.route('/api/stock', methods=['GET']) #API
def get_all_stock(): #Fetches stock for all products.
    """Get all current stock levels"""
    sql = """
    SELECT s.product_id, s.quantity, s.location, s.last_updated, p.name as product_name
    FROM stock s
    LEFT JOIN products p ON s.product_id = p.id
    ORDER BY s.last_updated DESC
    """
    result = execute_query(sql)
    
    if 'error' not in result:
        return jsonify({
            "success": True,
            "stock": result.get('dataset', []),
            "columns": [col['name'] for col in result.get('columns', [])]
        })
    else:
        return jsonify(result), 500

@app.route('/api/stock/update', methods=['POST']) #API
def update_stock(): #Handles stock changes: IN (add), OUT (subtract), or ADJUSTMENT (set exact value).
    """Update stock levels (handles IN/OUT/ADJUSTMENT)"""
    data = request.get_json()
    
    product_id = data.get('product_id')
    quantity_change = int(data.get('quantity', 0))
    transaction_type = data.get('type', 'ADJUSTMENT')  # IN, OUT, ADJUSTMENT
    location = data.get('location', 'Warehouse-A')
    reason = data.get('reason', 'Manual update')
    
    # Get current stock (Fetches latest stock quantity for that product)
    current_stock_sql = f"SELECT quantity FROM stock WHERE product_id = '{product_id}' ORDER BY last_updated DESC LIMIT 1"
    current_result = execute_query(current_stock_sql)
    
    current_quantity = 0
    if current_result.get('count', 0) > 0:
        current_quantity = current_result['dataset'][0][0]
    
    # Calculate new stock based on transaction type
    if transaction_type == 'IN':
        new_quantity = current_quantity + quantity_change
    elif transaction_type == 'OUT':
        new_quantity = current_quantity - quantity_change
    else:  # ADJUSTMENT
        new_quantity = quantity_change
    
    # Prevent negative stock
    if new_quantity < 0:
        return jsonify({"error": "Cannot have negative stock"}), 400  #If result is below zero , reject request with 400 Bad Request.
    
    # Create transaction ID
    txn_id = f"TXN{uuid.uuid4().hex[:8].upper()}"
    
    try:
        # Update stock table(insert the new stock record into stock table)
        stock_sql = f"""
        INSERT INTO stock VALUES (
            '{product_id}', {new_quantity}, '{location}', now()
        )
        """
        
        # Record transaction (log the transaction details in stock_transactions table.)
        transaction_sql = f"""
        INSERT INTO stock_transactions VALUES (
            '{txn_id}', '{product_id}', '{transaction_type}', 
            {quantity_change}, {current_quantity}, {new_quantity}, 
            '{reason}', now()
        )
        """
        
        # Execute both queries
        stock_result = execute_query(stock_sql)
        txn_result = execute_query(transaction_sql)
        
        if 'error' not in stock_result and 'error' not in txn_result:
            return jsonify({
                "success": True,
                "transaction_id": txn_id,
                "previous_stock": current_quantity,
                "new_stock": new_quantity,
                "change": quantity_change
            }), 200
        else:
            return jsonify({"error": "Failed to update stock"}), 500
            
    except Exception as e:
        return jsonify({"error": f"Update failed: {str(e)}"}), 500 #Catches unexpected errors and returns them as a 500 Internal Server Error

# ============================================================================
# TRANSACTION HISTORY APIS
# ============================================================================

@app.route('/api/transactions/<product_id>', methods=['GET'])
def get_transactions(product_id): #fetch past stock changes for one product
    """Get transaction history for a product"""
    limit = request.args.get('limit', 50)
    
    sql = f"""
    SELECT * FROM stock_transactions 
    WHERE product_id = '{product_id}' 
    ORDER BY timestamp DESC 
    LIMIT {limit}
    """
    result = execute_query(sql) #Executes the query on QuestDB.
    
    if 'error' not in result:
        return jsonify({
            "success": True,
            "transactions": result.get('dataset', []),
            "columns": [col['name'] for col in result.get('columns', [])]
        })
    else:
        return jsonify(result), 500

@app.route('/api/transactions', methods=['GET']) #Fetches transactions for all products.
def get_all_transactions():
    """Get all transactions"""
    limit = request.args.get('limit', 100)
    
    sql = f"""
    SELECT t.*, p.name as product_name
    FROM stock_transactions t
    LEFT JOIN products p ON t.product_id = p.id
    ORDER BY t.timestamp DESC 
    LIMIT {limit}
    """
    result = execute_query(sql)
    
    if 'error' not in result:
        return jsonify({
            "success": True,
            "transactions": result.get('dataset', []),
            "columns": [col['name'] for col in result.get('columns', [])]
        })
    else:
        return jsonify(result), 500

# ============================================================================
# HEALTH CHECK & INFO
# ============================================================================

@app.route('/api/health', methods=['GET']) #Used to check if the app and database are running properly
def health_check():
    """Health check endpoint"""
    db_result = execute_query("SELECT 1")
    
    return jsonify({
        "status": "healthy" if 'error' not in db_result else "unhealthy",
        "database": "connected" if 'error' not in db_result else "disconnected",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/dashboard', methods=['GET'])
def dashboard():
    """Dashboard summary""" #Provides a summary view of inventory
    # Get total products
    products_sql = "SELECT count(*) as total_products FROM products"
    products_result = execute_query(products_sql)
    
    # Get total stock value
    value_sql = """
    SELECT sum(s.quantity * p.unit_price) as total_value
    FROM stock s
    JOIN products p ON s.product_id = p.id
    """
    value_result = execute_query(value_sql)
    
    # Get low stock items (less than 10)
    low_stock_sql = """
    SELECT s.product_id, p.name, s.quantity
    FROM stock s
    JOIN products p ON s.product_id = p.id
    WHERE s.quantity < 10
    ORDER BY s.quantity ASC
    """
    low_stock_result = execute_query(low_stock_sql)
    
    return jsonify({
        "total_products": products_result['dataset'][0][0] if products_result.get('count', 0) > 0 else 0,
        "total_inventory_value": value_result['dataset'][0][0] if value_result.get('count', 0) > 0 else 0,
        "low_stock_items": low_stock_result.get('dataset', []),
        "timestamp": datetime.now().isoformat()
    })

if __name__ == '__main__':
    print("🚀 Starting Inventory Management API...")
    print("📊 Dashboard: http://localhost:5000/api/dashboard")
    print("🔍 Health: http://localhost:5000/api/health")
    print("📦 Products: http://localhost:5000/api/products")
    print("📈 Stock: http://localhost:5000/api/stock")
    
    app.run(host='0.0.0.0', port=5000, debug=True)
