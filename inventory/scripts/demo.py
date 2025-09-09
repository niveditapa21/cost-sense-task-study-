#!/usr/bin/env python3
"""
Inventory Management System Demo
Shows REST API and gRPC functionality working together
"""

import requests
import grpc
import inventory_pb2
import inventory_pb2_grpc
import json
import time

# Configuration
REST_BASE_URL = "http://localhost:5000/api"
GRPC_ADDRESS = "localhost:50051"

def print_separator(title):
    """Print a nice separator for demo sections"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)

def print_response(response_type, data):
    """Pretty print API responses"""
    print(f"\n[{response_type}] Response:")
    if isinstance(data, dict):
        print(json.dumps(data, indent=2))
    else:
        print(data)

def demo_rest_api():
    """Demonstrate REST API functionality"""
    print_separator("REST API DEMO")
    
    # 1. Check health
    print("\n🔍 Checking REST API health...")
    health_response = requests.get(f"{REST_BASE_URL}/health")
    print_response("HEALTH", health_response.json())
    
    # 2. Create a product via REST
    print("\n📦 Creating product via REST API...")
    product_data = {
        "name": "REST Laptop",
        "description": "Gaming laptop created via REST API",
        "category": "Electronics",
        "unit_price": 1299.99
    }
    create_response = requests.post(f"{REST_BASE_URL}/products", json=product_data)
    product_result = create_response.json()
    print_response("CREATE PRODUCT", product_result)
    
    if create_response.status_code == 201:
        product_id = product_result["product_id"]
        
        # 3. Get the product
        print(f"\n🔎 Getting product {product_id}...")
        get_response = requests.get(f"{REST_BASE_URL}/products/{product_id}")
        print_response("GET PRODUCT", get_response.json())
        
        # 4. Update stock
        print(f"\n📈 Adding stock for product {product_id}...")
        stock_data = {
            "product_id": product_id,
            "quantity": 50,
            "type": "IN",
            "reason": "Initial stock via REST API"
        }
        stock_response = requests.post(f"{REST_BASE_URL}/stock/update", json=stock_data)
        print_response("UPDATE STOCK", stock_response.json())
        
        # 5. Check stock level
        print(f"\n📊 Checking stock level for {product_id}...")
        stock_check = requests.get(f"{REST_BASE_URL}/stock/{product_id}")
        print_response("GET STOCK", stock_check.json())
        
        # 6. Create OUT transaction
        print(f"\n📉 Creating OUT transaction for {product_id}...")
        out_data = {
            "product_id": product_id,
            "quantity": 5,
            "type": "OUT",
            "reason": "Sale via REST API"
        }
        out_response = requests.post(f"{REST_BASE_URL}/stock/update", json=out_data)
        print_response("OUT TRANSACTION", out_response.json())
        
        # 7. Get transaction history
        print(f"\n📋 Getting transaction history for {product_id}...")
        history_response = requests.get(f"{REST_BASE_URL}/transactions/{product_id}")
        print_response("TRANSACTION HISTORY", history_response.json())
        
        return product_id
    else:
        print("❌ Failed to create product via REST API")
        return None

def demo_grpc_api():
    """Demonstrate gRPC API functionality"""
    print_separator("gRPC API DEMO")
    
    try:
        # Connect to gRPC server
        with grpc.insecure_channel(GRPC_ADDRESS) as channel:
            stub = inventory_pb2_grpc.InventoryServiceStub(channel)
            
            # 1. Create a product via gRPC
            print("\n📦 Creating product via gRPC...")
            create_request = inventory_pb2.CreateProductRequest(
                name="gRPC Smartphone",
                description="Latest smartphone created via gRPC",
                price=899.99
            )
            create_response = stub.CreateProduct(create_request)
            print_response("gRPC CREATE PRODUCT", {
                "product_id": create_response.product.id,
                "name": create_response.product.name,
                "message": create_response.message
            })
            
            if create_response.product.id:
                product_id = create_response.product.id
                
                # 2. Get the product via gRPC
                print(f"\n🔎 Getting product {product_id} via gRPC...")
                get_request = inventory_pb2.GetProductRequest(id=product_id)
                get_response = stub.GetProduct(get_request)
                print_response("gRPC GET PRODUCT", {
                    "product_id": get_response.product.id,
                    "name": get_response.product.name,
                    "description": get_response.product.description,
                    "price": get_response.product.price,
                    "message": get_response.message
                })
                
                # 3. Update stock via gRPC
                print(f"\n📈 Setting stock for {product_id} via gRPC...")
                stock_request = inventory_pb2.UpdateStockRequest(
                    product_id=product_id,
                    quantity=75
                )
                stock_response = stub.UpdateStock(stock_request)
                print_response("gRPC UPDATE STOCK", {
                    "product_id": stock_response.product_id,
                    "current_stock": stock_response.current_stock,
                    "message": stock_response.message
                })
                
                # 4. Get stock via gRPC
                print(f"\n📊 Getting stock for {product_id} via gRPC...")
                get_stock_request = inventory_pb2.GetStockRequest(product_id=product_id)
                get_stock_response = stub.GetStock(get_stock_request)
                print_response("gRPC GET STOCK", {
                    "product_id": get_stock_response.product_id,
                    "current_stock": get_stock_response.current_stock,
                    "message": get_stock_response.message
                })
                
                # 5. Create IN transaction via gRPC
                print(f"\n📈 Creating IN transaction for {product_id} via gRPC...")
                in_txn_request = inventory_pb2.CreateTransactionRequest(
                    product_id=product_id,
                    quantity=25,
                    transaction_type="IN"
                )
                in_txn_response = stub.CreateTransaction(in_txn_request)
                print_response("gRPC IN TRANSACTION", {
                    "transaction_id": in_txn_response.transaction_id,
                    "message": in_txn_response.message
                })
                
                # 6. Create OUT transaction via gRPC
                print(f"\n📉 Creating OUT transaction for {product_id} via gRPC...")
                out_txn_request = inventory_pb2.CreateTransactionRequest(
                    product_id=product_id,
                    quantity=10,
                    transaction_type="OUT"
                )
                out_txn_response = stub.CreateTransaction(out_txn_request)
                print_response("gRPC OUT TRANSACTION", {
                    "transaction_id": out_txn_response.transaction_id,
                    "message": out_txn_response.message
                })
                
                return product_id
            else:
                print("❌ Failed to create product via gRPC")
                return None
                
    except grpc.RpcError as e:
        print(f"❌ gRPC Error: {e.details()}")
        return None
    except Exception as e:
        print(f"❌ Connection Error: {str(e)}")
        return None

def demo_dashboard():
    """Show dashboard summary"""
    print_separator("DASHBOARD SUMMARY")
    
    try:
        dashboard_response = requests.get(f"{REST_BASE_URL}/dashboard")
        if dashboard_response.status_code == 200:
            print_response("DASHBOARD", dashboard_response.json())
        else:
            print("❌ Failed to get dashboard data")
            
        # Get all products
        print("\n📦 All Products:")
        products_response = requests.get(f"{REST_BASE_URL}/products")
        if products_response.status_code == 200:
            products_data = products_response.json()
            if products_data.get("products"):
                for i, product in enumerate(products_data["products"][:5], 1):  # Show first 5
                    print(f"  {i}. {product[1]} - ${product[4]} (ID: {product[0]})")
            else:
                print("  No products found")
        
        # Get all stock
        print("\n📊 Current Stock Levels:")
        stock_response = requests.get(f"{REST_BASE_URL}/stock")
        if stock_response.status_code == 200:
            stock_data = stock_response.json()
            if stock_data.get("stock"):
                for i, stock in enumerate(stock_data["stock"][:5], 1):  # Show first 5
                    print(f"  {i}. {stock[4] or stock[0]} - Stock: {stock[1]} units")
            else:
                print("  No stock records found")
                
    except Exception as e:
        print(f"❌ Error getting dashboard: {str(e)}")

def main():
    """Main demo function"""
    print("🚀 INVENTORY MANAGEMENT SYSTEM DEMO")
    print("=" * 60)
    print("This demo shows both REST API and gRPC functionality")
    print("Make sure both servers are running:")
    print("  - REST API: http://localhost:5000")
    print("  - gRPC Server: localhost:50051")
    print("  - QuestDB: http://localhost:9000")
    
    input("\nPress Enter to start the demo...")
    