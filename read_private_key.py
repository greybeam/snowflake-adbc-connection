from cryptography.hazmat.primitives import serialization

def read_private_key(private_key_path: str, private_key_passphrase: str = None) -> str:
    """Read a private key file and return its PEM representation as a string"""
    with open(private_key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=private_key_passphrase.encode() if private_key_passphrase else None
        )
        
        # Convert to PEM format with PKCS8
        pem_key = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return pem_key.decode('utf-8')