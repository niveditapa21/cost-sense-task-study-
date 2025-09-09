#!/usr/bin/env python3
"""
gRPC Inventory Management Server
Connects to QuestDB for time-series inventory tracking
"""

import grpc
from concurrent import futures
import requests #For connecting to QuestDB REST API
import uuid #Generates unique IDs (e.g., for products/transactions)
from datetime import datetime

import inventory_pb2 #contains message classes (like Product, StockResponse, etc.)
import inventory_pb2_grpc #contains service definitions (server & client stubs

# QuestDB connection settings
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

class InventoryServicer(inventory_pb2_grpc.InventoryServiceServicer):
    
    def CreateProduct(self, request, context):
        """Create a new product"""
        try:
            product_id = f"PROD{uuid.uuid4().hex[:6].upper()}"
            name = request.name
            description = request.description
            price = request.price
            
            sql = f"""
            INSERT INTO products VALUES (
                '{product_id}', '{name}', '{description}', '', 
                {price}, now(), now()
            )
            """
            
            result = execute_query(sql)
            
            if 'error' not in result:
                product = inventory_pb2.Product(
                    id=product_id,
                    name=name,
                    description=description,
                    price=price
                )
                return inventory_pb2.ProductResponse(
                    product=product,
                    message="Product created successfully"
                )
            else:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(result['error'])
                return inventory_pb2.ProductResponse(message=result['error'])
                
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return inventory_pb2.ProductResponse(message=f"Error: {str(e)}")
    
    def GetProduct(self, request, context):
        """Get a specific product"""
        try:
            product_id = request.id
            sql = f"SELECT * FROM products WHERE id = '{product_id}'"
            result = execute_query(sql)
            
            if 'error' not in result and result.get('count', 0) > 0:
                data = result['dataset'][0]
                product = inventory_pb2.Product(
                    id=data[0],
                    name=data[1],
                    description=data[2],
                    price=float(data[4])
                )
                return inventory_pb2.ProductResponse(
                    product=product,
                    message="Product found"
                )
            else:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Product not found")
                return inventory_pb2.ProductResponse(message="Product not found")
                
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return inventory_pb2.ProductResponse(message=f"Error: {str(e)}")
    
    def GetStock(self, request, context):
        """Get current stock for a product"""
        try:
            product_id = request.product_id
            sql = f"SELECT * FROM stock WHERE product_id = '{product_id}' ORDER BY last_updated DESC LIMIT 1"
            result = execute_query(sql)
            
            if 'error' not in result and result.get('count', 0) > 0:
                current_stock = int(result['dataset'][0][1])
                return inventory_pb2.StockResponse(
                    product_id=product_id,
                    current_stock=current_stock,
                    message="Stock retrieved"
                )
            else:
                return inventory_pb2.StockResponse(
                    product_id=product_id,
                    current_stock=0,
                    message="No stock record found"
                )
                
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return inventory_pb2.StockResponse(
                product_id=request.product_id,
                current_stock=0,
                message=f"Error: {str(e)}"
            )
    
    def UpdateStock(self, request, context):
        """Update stock levels"""
        try:
            product_id = request.product_id
            quantity_change = request.quantity
            
            # Get current stock
            current_stock_sql = f"SELECT quantity FROM stock WHERE product_id = '{product_id}' ORDER BY last_updated DESC LIMIT 1"
            current_result = execute_query(current_stock_sql)
            
            current_quantity = 0
            if current_result.get('count', 0) > 0:
                current_quantity = int(current_result['dataset'][0][0])
            
            # For simplicity, treat this as setting absolute stock level
            new_quantity = quantity_change
            
            if new_quantity < 0:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Cannot have negative stock")
                return inventory_pb2.StockResponse(
                    product_id=product_id,
                    current_stock=current_quantity,
                    message="Cannot have negative stock"
                )
            
            # Update stock
            stock_sql = f"""
            INSERT INTO stock VALUES (
                '{product_id}', {new_quantity}, 'Warehouse-A', now()
            )
            """
            
            result = execute_query(stock_sql)
            
            if 'error' not in result:
                return inventory_pb2.StockResponse(
                    product_id=product_id,
                    current_stock=new_quantity,
                    message="Stock updated successfully"
                )
            else:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(result['error'])
                return inventory_pb2.StockResponse(
                    product_id=product_id,
                    current_stock=current_quantity,
                    message=result['error']
                )
                
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return inventory_pb2.StockResponse(
                product_id=request.product_id,
                current_stock=0,
                message=f"Error: {str(e)}"
            )
    
    def CreateTransaction(self, request, context):
        """Create a stock transaction"""
        try:
            product_id = request.product_id
            quantity = request.quantity
            transaction_type = request.transaction_type
            
            # Generate transaction ID
            txn_id = f"TXN{uuid.uuid4().hex[:8].upper()}"
            
            # Get current stock for reference
            current_stock_sql = f"SELECT quantity FROM stock WHERE product_id = '{product_id}' ORDER BY last_updated DESC LIMIT 1"
            current_result = execute_query(current_stock_sql)
            
            current_quantity = 0
            if current_result.get('count', 0) > 0:
                current_quantity = int(current_result['dataset'][0][0])
            
            # Calculate new stock
            if transaction_type == 'IN':
                new_quantity = current_quantity + quantity
            elif transaction_type == 'OUT':
                new_quantity = current_quantity - quantity
                if new_quantity < 0:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("Insufficient stock")
                    return inventory_pb2.TransactionResponse(
                        transaction_id="",
                        message="Insufficient stock for OUT transaction"
                    )
            else:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Invalid transaction type")
                return inventory_pb2.TransactionResponse(
                    transaction_id="",
                    message="Transaction type must be 'IN' or 'OUT'"
                )
            
            # Update stock
            stock_sql = f"""
            INSERT INTO stock VALUES (
                '{product_id}', {new_quantity}, 'Warehouse-A', now()
            )
            """
            
            # Record transaction
            transaction_sql = f"""
            INSERT INTO stock_transactions VALUES (
                '{txn_id}', '{product_id}', '{transaction_type}', 
                {quantity}, {current_quantity}, {new_quantity}, 
                'gRPC transaction', now()
            )
            """
            
            # Execute both queries
            stock_result = execute_query(stock_sql)
            txn_result = execute_query(transaction_sql)
            
            if 'error' not in stock_result and 'error' not in txn_result:
                return inventory_pb2.TransactionResponse(
                    transaction_id=txn_id,
                    message=f"Transaction completed. Stock: {current_quantity} -> {new_quantity}"
                )
            else:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Failed to complete transaction")
                return inventory_pb2.TransactionResponse(
                    transaction_id="",
                    message="Failed to complete transaction"
                )
                
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return inventory_pb2.TransactionResponse(
                transaction_id="",
                message=f"Error: {str(e)}"
            )

def serve():
    """Start the gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryServicer(), server)
    
    listen_addr = '[::]:50051'
    server.add_insecure_port(listen_addr)
    
    print("🚀 Starting gRPC Inventory Server...")
    print(f"📡 Listening on {listen_addr}")
    print("🔗 Connected to QuestDB at localhost:9000")
    
    server.start()
    print("✅ gRPC Server started successfully!")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n⏹️  Shutting down gRPC server...")
        server.stop(0)

if __name__ == '__main__':
    serve()
