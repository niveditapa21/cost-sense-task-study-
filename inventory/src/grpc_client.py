import grpc
import inventory_pb2
import inventory_pb2_grpc

def test_client():
    # Connect to gRPC server
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = inventory_pb2_grpc.InventoryServiceStub(channel)
        
        # Test creating a product
        request = inventory_pb2.CreateProductRequest(
            name="Test Product",
            description="This is a test product",
            price=99.99
        )
        
        response = stub.CreateProduct(request)
        print(f"Create Product: {response.message}")
        print(f"Product ID: {response.product.id}")
        
        # Test getting the product
        get_request = inventory_pb2.GetProductRequest(id=response.product.id)
        get_response = stub.GetProduct(get_request)
        print(f"Get Product: {get_response.message}")
        
        # Test updating stock
        stock_request = inventory_pb2.UpdateStockRequest(product_id=response.product.id, quantity=100)
        stock_response = stub.UpdateStock(stock_request)
        print(f"Update Stock: {stock_response.message}")

if __name__ == '__main__':
    test_client()