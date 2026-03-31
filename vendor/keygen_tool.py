import json
import base64
import os
import datetime
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import serialization

# --- VENDOR KEY GENERATOR TOOL ---
# Keep this script and the private_key.pem PRIVATE.

def generate_keys():
    """Generates a new RSA key pair."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    
    # Save Private Key
    with open("private_key.pem", "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))
    
    # Save Public Key (to be embedded in license_utils.py)
    public_key = private_key.public_key()
    with open("public_key.pem", "wb") as f:
        f.write(public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ))
    print("Keys generated: private_key.pem, public_key.pem")

def create_license(hwids, expires_days=30):
    """Signs a license for the given HWIDs."""
    if not os.path.exists("private_key.pem"):
        print("Error: private_key.pem not found. Run generate_keys first.")
        return

    # Load Private Key
    with open("private_key.pem", "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    
    # License Data
    # 默认有效期：1 个月（30 天）
    expires = (datetime.datetime.now() + datetime.timedelta(days=expires_days)).strftime("%Y-%m-%d")
    data = {
        "hwids": hwids if isinstance(hwids, list) else [hwids],
        "expires": expires,
        "version": "1.0"
    }
    
    data_json = json.dumps(data)
    data_b64 = base64.b64encode(data_json.encode()).decode()
    
    # Sign Data
    signature = private_key.sign(
        data_json.encode(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    sig_b64 = base64.b64encode(signature).decode()
    
    license_str = f"{data_b64}.{sig_b64}"
    
    with open("license.dat", "w") as f:
        f.write(license_str)
    
    print(f"License created for {hwids} (expires {expires}) -> license.dat")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python keygen_tool.py init         # Generate RSA keys")
        print("  python keygen_tool.py sign <HWID> [days]  # Create license for HWID (default 30 days)")
    elif sys.argv[1] == "init":
        generate_keys()
    elif sys.argv[1] == "sign":
        args = sys.argv[2:]
        days = 30
        if len(args) >= 2 and args[-1].isdigit():
            days = int(args[-1])
            args = args[:-1]
        create_license(args, expires_days=days)
