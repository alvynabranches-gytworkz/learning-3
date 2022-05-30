import boto3
import json
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = "product-inventory"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(dynamodbTableName)

getMethod = "GET"
postMethod = "POST"
patchMethod = "PATCH"
deleteMethod = "DELETE"

healthPath = "/health"
productPath = "/product"
productsPath = "/products"

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)

def buildResponse(statusCode, body=None):
    response = {
        "statusCode": statusCode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        }
    }
    
    if body is not None:
        response["body"] = json.dumps(body, cls=CustomEncoder)
    
    return response

def getProduct(productId):
    try:
        response = table.get_item(
            Key={
                "productId": productId
            }
        )
        if "Item" in response:
            return buildResponse(200, response["Item"])
        return buildResponse(404, {"message": f"ProductId: {productId} not found."})
    except Exception as e:
        logger.exception("Error retrieving data!", e)
    return buildResponse(404, {"message": f"Invalid productId: {productId}!"})

def getProducts():
    try:
        response = table.scan()
        result = response["Item"]
        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            result.extend(response["Item"])
        
        body = {
            "products": response
        }
        return buildResponse(200, body)
    except Exception as e:
        logging.exception("Error retrieving data!", e)
    return 

def saveProduct(requestBody):
    try:
        table.put_item(Item=requestBody)
        body = {
            "Operation": "SAVE",
            "Message": "SUCCESS",
            "Item": requestBody
        }
        return buildResponse(201, body)
    except Exception as e:
        logging.exception("Error saving data!", e)

def modifyProduct(productId, updateKey, updateValue):
    try:
        response = table.update_item(
            Key={
                "productId": productId
            },
            UpdateExpression=f"set {updateKey} = :value",
            ExpressionAttributeValues={
                ":value": updateValue
            },
            ReturnValues="UPDATED_NEW"
        )
        body = {
            "Operation": "UPDATE",
            "Message": "SUCCESS",
            "UpdatedAttributes": response
        }
        return buildResponse(201, body)
    except Exception as e:
        logging.exception("Error updating data!", e)

def deleteProduct(productId):
    try:
        response = table.delete_item(
            Key={
                "productId": productId
            },
            ReturnValues="ALL_OLD"
        )
        body = {
            "Operation": "DELETE",
            "Message": "SUCCESS",
            "deletedItems": response
        }
        return buildResponse(201, body)
    except Exception as e:
        logging.exception("Error deleting data!", e)

def lambda_handler(event, context):
    logger.info(event)
    httpMethod = event["httpMethod"]
    path = event["path"]
    
    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200)
    elif httpMethod == getMethod and path == productPath:
        response = getProduct(event["queryStringParameters"]["productId"])
    elif httpMethod == getMethod and path == productsPath:
        response = getProducts()
    elif httpMethod == postMethod and path == productPath:
        response = saveProduct(json.loads(event["body"]))
    elif httpMethod == patchMethod and path == productPath:
        requestBody = json.loads(event["body"])
        response = modifyProduct(requestBody["productId"], requestBody["updateKey"], requestBody["updateValue"])
    elif httpMethod == deleteMethod and path == productPath:
        requestBody = event["body"]
        response = deleteProduct(requestBody["productId"])
    else:
        response = buildResponse(404)
    
    return response
