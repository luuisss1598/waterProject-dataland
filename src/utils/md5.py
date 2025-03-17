import hashlib

def generate_md5_key(value1: any, value2: any) -> str:
    
    combine_values: str = str(value1) + str(value2)
    encoded_combine_values = combine_values.encode()
    
    # return result as a hexdecimal
    md5_result = hashlib.md5(encoded_combine_values)
    return md5_result.hexdigest()
