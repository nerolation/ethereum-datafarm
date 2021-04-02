import sha3
   
def get_method_from_canonical_expression(method):
    return '0x' + sha3.keccak_256(method.encode('utf-8')).hexdigest()

class Method:
    def __init__(self, method):
        self.canonicalExpression = method
        self.id = get_method_from_canonical_expression(method)
        self.simpleExp = method.split("(")[0].lower()