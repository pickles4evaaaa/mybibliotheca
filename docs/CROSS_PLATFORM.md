# Cross-Platform Compatibility Guide

This document outlines how Bibliotheca handles differences between operating systems to ensure consistent behavior across Windows, macOS, and Linux.

## 🖥️ Platform-Specific Considerations

### **File Permissions**

**Unix-like Systems (Linux/macOS):**
- Uses octal permissions: `755` for directories, `664` for database files
- Permissions are enforced to ensure proper access control
- Compatible with Docker container permissions

**Windows:**
- Skips Unix-style permission setting (not applicable)
- Relies on Windows default file permissions
- Directory and file creation still works correctly

### **Path Handling**

**All Platforms:**
- Uses `os.path.join()` and `pathlib.Path` for cross-platform path construction
- Automatically handles forward slashes (Unix) vs backslashes (Windows)
- Database paths are constructed dynamically to work on all systems

### **Directory Setup**

**Automated Setup Scripts:**

1. **`setup_data_dir.py`** - Cross-platform Python script (recommended)
   - Detects operating system automatically
   - Applies appropriate permissions based on platform
   - Works identically on Windows, macOS, and Linux

2. **`setup_data_dir.bat`** - Windows batch script (alternative)
   - Native Windows batch file for users who prefer it
   - Creates same directory structure as other platforms

## 🔧 Technical Implementation

### **Platform Detection**
```python
import platform
system = platform.system()  # Returns: 'Windows', 'Darwin', or 'Linux'
```

### **Conditional Permission Setting**
```python
if system != "Windows":
    data_dir.chmod(0o755)  # Only set Unix permissions on Unix-like systems
    db_path.chmod(0o664)
else:
    # Skip permission setting on Windows
    pass
```

### **Cross-Platform Database Path**
```python
# Works on all platforms
DATABASE_PATH = os.path.join(basedir, 'data', 'books.db')
SQLALCHEMY_DATABASE_URI = f"sqlite:///{DATABASE_PATH}"
```

## 🚀 Running on Different Platforms

### **Linux/macOS**
```bash
# Setup
python3 setup_data_dir.py

# Run
gunicorn -w 4 -b 0.0.0.0:5054 run:app
```

### **Windows**
```cmd
# Setup (choose one)
python setup_data_dir.py
# OR
setup_data_dir.bat

# Run (choose one)
python -m gunicorn -w 4 -b 0.0.0.0:5054 run:app
# OR
gunicorn -w 4 -b 0.0.0.0:5054 run:app
```

## 🐳 Docker Consistency

The standalone setup is designed to match the Docker environment:

**Docker Environment:**
- Creates `/app/data` directory with `755` permissions
- Creates empty `books.db` file with `664` permissions
- Uses `chown` to set ownership (Linux containers)

**Standalone Environment:**
- Creates `./data` directory with appropriate platform permissions
- Creates empty `books.db` file with appropriate platform permissions
- Ensures same directory structure and file presence

## ✅ Tested Compatibility

This cross-platform approach has been designed to work on:

- **Linux distributions** (Ubuntu, Debian, CentOS, etc.)
- **macOS** (Intel and Apple Silicon)
- **Windows 10/11** (with Python 3.8+)
- **Docker** (all platforms with Docker Desktop)

## 🔍 Troubleshooting

### **Permission Errors on Unix-like Systems**
- The setup script gracefully handles permission errors
- If you encounter issues, ensure you have write access to the project directory
- Consider running with appropriate user permissions

### **Gunicorn on Windows**
- If `gunicorn` command fails, use `python -m gunicorn` instead
- Ensure gunicorn is installed: `pip install gunicorn`
- Windows may require additional setup for some WSGI servers

### **Path Issues**
- All paths are constructed using `os.path.join()` for cross-platform compatibility
- If you encounter path-related errors, check that the project directory structure is intact

This approach ensures that whether you're running on Windows, macOS, Linux, or in Docker, Bibliotheca behaves consistently and maintains the same directory structure and permissions appropriate for each platform.
