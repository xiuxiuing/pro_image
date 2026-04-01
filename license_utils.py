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
    def verify_license_detailed(license_content, current_hwid):
        """
        Verifies license and returns structured info for UI.
        Returns dict: valid (bool), message (str), expires (str|None), days_remaining (int|None).
        days_remaining is calendar days from today to expiry date (inclusive of expiry day as last valid day).
        """
        import datetime
        err = {"valid": False, "message": "", "expires": None, "days_remaining": None}
        try:
            public_key = serialization.load_pem_public_key(LicenseManager.PUBLIC_KEY_PEM)
            parts = license_content.split(".")
            if len(parts) != 2:
                err["message"] = "Invalid license format"
                return err

            data_b64, sig_b64 = parts
            data_json = base64.b64decode(data_b64).decode()
            signature = base64.b64decode(sig_b64)

            public_key.verify(
                signature,
                data_json.encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256(),
            )

            data = json.loads(data_json)
            allowed_hwids = data.get("hwids", [])
            if current_hwid not in allowed_hwids:
                err["message"] = "This license is not for this machine"
                return err

            exp_str = data.get("expires", "2099-12-31")
            exp_dt = datetime.datetime.strptime(exp_str, "%Y-%m-%d")
            exp_date = exp_dt.date()
            today = datetime.date.today()
            days_remaining = (exp_date - today).days

            if datetime.datetime.now() > exp_dt:
                err["message"] = "License has expired"
                err["expires"] = exp_str
                err["days_remaining"] = max(0, days_remaining)
                return err

            return {
                "valid": True,
                "message": "Valid License",
                "expires": exp_str,
                "days_remaining": days_remaining,
            }
        except Exception as e:
            err["message"] = f"Verification failed: {str(e)}"
            return err

    @staticmethod
    def verify_license(license_content, current_hwid):
        """Verifies an RSA-signed license file."""
        d = LicenseManager.verify_license_detailed(license_content, current_hwid)
        return d["valid"], d["message"]

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
