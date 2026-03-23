import hashlib
import platform
import subprocess
import json
import base64
import os
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization

class LicenseManager:
    # Public Key used to verify licenses (matches the private key in keygen_tool.py)
    PUBLIC_KEY_PEM = b"""-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAwMtLdeFl/6bb7Tj0cJMp
ZSHG9e9k1YNT7yHlnYgSf2C9739hEpnVI1nlnMxPtE9VfdoiHsBhoIwNk9DQa+JR
42+hCz05RwIiBXektfowybnYHXum7qW4s/4syxowCt0GxzHdrlDke/oPgQZi476N
oDIVTwpRrp7cgtomLGZdpSPteMs6RTgTqAJSzI+mcn4scu+ABbm/eToLvjVrEWhH
btmhi8ikRoKP/ATFs5J21qdcntw/8ZduQpRJ+kfNKuX07eCI5+zsHOhFSsao2pc5
3xD/eRnbr+O11RiWkjaYaCMovJaIV6r4uEMrB944xgyL+xekTcXmC4dQGpe1w7BP
40I7LNa+c6HZC9oTaTjBhhrjt//+eqZyvUkER24vg18RZKjNxXqrmTtLIfcvA3pU
YrgTatLYSDMTvuPu/mP841aGz2xYqFS+MbEZMwBSoKl2hqRvWA6nCsI5Abmqi5QE
g3T5KtVuokKdWlnbRyeolfxbAAyfPbeZwAvpxJ9sazTnh7WJNEohLqdWbUyyaES7
rEA/EosjwWacWs4dwjXtWmbA6RRyR7rn+YlZvTlnVPD1+5xW5C9DbIX/9ERXZP1v
HJhi2FcM8wJAvKeBdcfgVa2oJwE6eRMy5rZoOJR2wK0VWLl8Er1QXqAOe2miiHwH
nTsQck+U8NXCoiCbWJVt6EcCAwEAAQ==
-----END PUBLIC KEY-----"""

    @staticmethod
    def get_hwid():
        """Generates a unique hardware ID for this machine."""
        system = platform.system()
        hw_info = []
        
        try:
            if system == "Windows":
                # CPU ID and Disk Serial
                hw_info.append(subprocess.check_output("wmic cpu get processorid", shell=True).decode().split('\n')[1].strip())
                hw_info.append(subprocess.check_output("wmic diskdrive get serialnumber", shell=True).decode().split('\n')[1].strip())
            elif system == "Darwin": # Mac
                # IOPlatformSerialNumber
                hw_info.append(subprocess.check_output("ioreg -l | grep IOPlatformSerialNumber", shell=True).decode().split('"')[-2])
            else: # Linux
                with open("/etc/machine-id", "r") as f:
                    hw_info.append(f.read().strip())
        except Exception as e:
            # Fallback to UUID
            import uuid
            hw_info.append(str(uuid.getnode()))

        # Hash the combined info
        raw_id = "|".join(hw_info)
        return hashlib.sha256(raw_id.encode()).hexdigest().upper()

    @staticmethod
    def verify_license(license_content, current_hwid):
        """Verifies an RSA-signed license file."""
        try:
            # Load Public Key
            public_key = serialization.load_pem_public_key(LicenseManager.PUBLIC_KEY_PEM)
            
            # Decode license (format: BASE64(JSON_DATA) + "." + BASE64(SIGNATURE))
            parts = license_content.split(".")
            if len(parts) != 2:
                return False, "Invalid license format"
            
            data_b64, sig_b64 = parts
            data_json = base64.b64decode(data_b64).decode()
            signature = base64.b64decode(sig_b64)
            
            # Verify Signature
            public_key.verify(
                signature,
                data_json.encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            
            # Signature OK, now check data
            data = json.loads(data_json)
            
            # 1. Check HWID (Supports multiple IDs)
            allowed_hwids = data.get("hwids", [])
            if current_hwid not in allowed_hwids:
                return False, "This license is not for this machine"
            
            # 2. Check Expiration
            import datetime
            exp_date = datetime.datetime.strptime(data.get("expires", "2099-12-31"), "%Y-%m-%d")
            if datetime.datetime.now() > exp_date:
                return False, "License has expired"
            
            return True, "Valid License"
            
        except Exception as e:
            return False, f"Verification failed: {str(e)}"

    @staticmethod
    def check_anti_debug():
        """Simple anti-debugging checks."""
        # This is a basic implementation; more advanced checks would be added here
        try:
            import sys
            if hasattr(sys, 'gettrace') and sys.gettrace() is not None:
                return True
        except:
            pass
        return False
