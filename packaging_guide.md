# Packaging & Obfuscation Guide

To protect your software from cracking and unauthorized redistribution, follow these steps to obfuscate the code and package it into a single EXE.

## Prerequisites
Install the required tools:
```bash
pip install pyarmor pyinstaller
```

## Step 1: Obfuscate the Code (PyArmor)
PyArmor obfuscates the Python bytecode, making it unreadable to humans and decompilers.

1.  **Obfuscate the core logic**:
    ```bash
    pyarmor gen -O dist/obfuscated app.py data_mgr.py license_utils.py main_030822.py extract_info_ai2.py
    ```
    This creates an `obfuscated` directory containing the protected files and a `pyarmor_runtime` package.

## Step 2: Package into EXE (PyInstaller)
Use PyInstaller to bundle everything into a single Windows executable.

1.  **Create the EXE**:
    ```bash
    pyinstaller --onefile --noconsole \
        --add-data "templates:templates" \
        --add-data "static:static" \
        --hidden-import cryptography \
        --name "ProImage_AI" \
        dist/obfuscated/app.py
    ```

## Step 3: Distribution
When distributing the software:
1.  Provide the `ProImage_AI.exe`.
2.  Do **NOT** include `private_key.pem` or `keygen_tool.py`.
3.  Instruct the user to run the app, copy their **HWID**, and send it to you.
4.  Use `keygen_tool.py sign <HWID>` to generate a `license.dat` and send it back to them.

## Security Recommendations
- **Rotate Keys**: Generate a new RSA key pair for each major version using `keygen_tool.py init`.
- **HWID Binding**: The current HWID uses `ioreg` (Mac) or `wmic` (Windows). Ensure you have admin privileges if using low-level hardware calls.
- **Timestamp Check**: The system includes an expiration date. Regularly check if the system clock has been tampered with (e.g., comparing with file modification times).
