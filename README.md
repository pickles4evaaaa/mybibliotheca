# 📚 MyBibliotheca

**MyBibliotheca** is a self-hosted personal library and reading tracker web app built with Flask. It lets you log, organize, and visualize your reading journey. Add books by ISBN, track reading progress, log daily reading, and generate monthly wrap-up images of your finished titles.


🆕 **Multi-User Features**: Multi-user authentication, user data isolation, admin management, and secure password handling.

=======
[**Join us on Discord!**](https://discord.gg/Hc8C5eRm7Q)

---

## ✨ Features

- 📖 **Add Books**: Add books quickly by ISBN with automatic cover and metadata fetching. Now featuring bulk-import from Goodreads and other CSV files! 
- ✅ **Track Progress**: Mark books as *Currently Reading*, *Want to Read*, *Finished*, or *Library Only*.
- 📅 **Reading Logs**: Log daily reading activity and maintain streaks.
- 🖼️ **Monthly Wrap-Ups**: Generate shareable image collages of books completed each month.
- 🔎 **Search**: Find and import books using the Google Books API.
- 📱 **Responsive UI**: Clean, mobile-friendly interface built with Bootstrap.
- 🔐 **Multi-User Support**: Secure authentication with user data isolation
- 👤 **Admin Management**: Administrative tools and user management

---

## 🖼️ Preview

![App Preview](https://i.imgur.com/AkiBN68.png)  
![Library](https://i.imgur.com/h9iR9ql.png)

---

## 🚀 Getting Started

### 📦 Run with Docker

MyBibliotheca can be run completely in Docker — no need to install Python or dependencies on your machine.

#### ✅ Prerequisites

- [Docker](https://www.docker.com/) installed
- [Docker Compose](https://docs.docker.com/compose/) installed

---

#### 🔁 Option 1: One-liner (Docker only)

```bash
docker run -d \
  --name mybibliotheca \
  -p 5054:5054 \
  -v /path/to/data:/app/data \
  -e TIMEZONE=America/Chicago \
  -e WORKERS=6 \
  --restart unless-stopped \
  pickles4evaaaa/mybibliotheca:latest
```

---

#### 🔁 Option 2: Docker Compose

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  bibliotheca:
    image: pickles4evaaaa/mybibliotheca:latest
    container_name: mybibliotheca
    ports:
      - "5054:5054"
    volumes:
      - /path/to/data:/app/data      # ← bind-mount host
    restart: unless-stopped
    environment:
      - TIMEZONE=America/Chicago  # ✅ Set your preferred timezone here
      - WORKERS=6  # Change to the number of Gunicorn workers you want
```

Then run:

```bash
docker compose up -d
```
### 🔧 Configurable Environment Variables

| Variable              | Description                                | Default / Example         |
|-----------------------|--------------------------------------------|---------------------------|
| `SECRET_KEY`          | Flask secret key for sessions             | `auto-generated`          |
| `SECURITY_PASSWORD_SALT` | Password hashing salt               | `auto-generated`          |
| `TIMEZONE`            | Sets the app's timezone                    | `America/Chicago`         |
| `WORKERS`             | Number of Gunicorn worker processes        | `6`                      |

---

## 🔐 Authentication & User Management

### First Time Setup

When you first run MyBibliotheca, you'll be prompted to complete a one-time setup:

1. **Access the application** at `http://localhost:5054` (or your configured port)
2. **Complete the setup form** to create your administrator account:
   - Choose an admin username
   - Provide an admin email address  
   - Set a secure password (must meet security requirements)
3. **Start using MyBibliotheca** - you'll be automatically logged in after setup

✅ **Secure by Design**: No default credentials - you control your admin account from the start!

### Password Security

- **Strong password requirements**: All passwords must meet security criteria
- **Automatic password changes**: New users are prompted to change their password on first login
- **Secure password storage**: All passwords are hashed using industry-standard methods

### Admin Tools

Use the built-in admin tools for password management:

```bash
# Reset admin password (interactive)
docker exec -it bibliotheca python3 admin_tools.py reset-admin-password

# Create additional admin user
docker exec -it bibliotheca python3 admin_tools.py create-admin

# List all users
docker exec -it bibliotheca python3 admin_tools.py list-users

# System statistics
docker exec -it bibliotheca python3 admin_tools.py system-stats
```

### Migration from V1.x

Existing single-user installations are **automatically migrated** to multi-user:
- **Automatic database backup** created before migration
- All existing books are assigned to an admin user (created via setup)
- No data is lost during migration
- V1.x functionality remains unchanged
- **Setup required** if no admin user exists after migration

📖 **Documentation:**
- **[MIGRATION.md](MIGRATION.md)** - Automatic migration system details
- **[AUTHENTICATION.md](AUTHENTICATION.md)** - Complete authentication guide
- **[ADMIN_TOOLS.md](ADMIN_TOOLS.md)** - Admin tools and user management
- **[TESTING.md](TESTING.md)** - Comprehensive testing documentation and procedures

---

### 🐍 Install from Source (Manual Setup)

#### ✅ Prerequisites

* Python 3.8+
* `pip`

---

### 🔧 Manual Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/pickles4evaaaa/mybibliotheca.git
   cd bibliotheca
   ```

2. **Create a Python virtual environment**

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Setup data directory** (ensures parity with Docker environment)

   **On Linux/macOS:**
   ```bash
   python3 setup_data_dir.py
   ```

   **On Windows:**
   ```cmd
   # Option 1: Use Python script (recommended)
   python setup_data_dir.py
   
   # Option 2: Use Windows batch script
   setup_data_dir.bat
   ```

   This step creates the `data` directory and database file with proper permissions for your platform.

   This step creates the `data` directory and database file with proper permissions for your platform.

5. **Run the app**

   **On Linux/macOS:**
   ```bash
   gunicorn -w NUMBER_OF_WORKERS -b 0.0.0.0:5054 run:app
   ```

   **On Windows:**
   ```cmd
   # If gunicorn is installed globally
   gunicorn -w NUMBER_OF_WORKERS -b 0.0.0.0:5054 run:app
   
   # Or use Python module (more reliable on Windows)
   python -m gunicorn -w NUMBER_OF_WORKERS -b 0.0.0.0:5054 run:app
   ```

   Visit: [http://127.0.0.1:5054](http://127.0.0.1:5054)

> 💡 No need to manually set up the database — it is created automatically on first run.

---

### ⚙️ Configuration

* By default, uses Redis for data storage and a secure auto-generated secret key.
* For production, you can configure:

  * `SECRET_KEY`
  * `REDIS_URL`
    via environment variables or `.env`.

---

## 🚀 Production Deployment

### Quick Production Setup

1. **Clone and configure**:
```bash
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca
cp .env.example .env
```

2. **Generate secure keys**:
```bash
# Generate SECRET_KEY
python3 -c "import secrets; print('SECRET_KEY=' + secrets.token_urlsafe(32))" >> .env

# Generate SECURITY_PASSWORD_SALT  
python3 -c "import secrets; print('SECURITY_PASSWORD_SALT=' + secrets.token_urlsafe(32))" >> .env
```

3. **Customize configuration** (edit `.env`):
```bash
# Set your timezone
TIMEZONE=America/Chicago

# Adjust workers based on your server
WORKERS=4
```

4. **Deploy**:
```bash
docker compose up -d
```

5. **Complete setup**: Visit your application and create your admin account through the setup page

### Production Security Checklist

- ✅ **Environment Variables**: Use `.env` file with secure random keys
- ✅ **HTTPS**: Deploy behind reverse proxy with SSL/TLS (nginx, Traefik, etc.)
- ✅ **Firewall**: Restrict access to necessary ports only
- ✅ **Backups**: Implement regular database backups
- ✅ **Updates**: Keep Docker images and host system updated
- ✅ **Monitoring**: Set up health checks and log monitoring

### Development Setup

For development and testing, use the development compose file:

```bash
# Development with live code reloading
docker compose -f docker-compose.dev.yml up -d

# Run tests
docker compose -f docker-compose.dev.yml --profile test up bibliotheca-test
```

---

## 🗂️ Project Structure

```
mybibliotheca/
├── app/
│   ├── __init__.py
│   ├── models.py
│   ├── routes.py
│   ├── utils.py
│   └── templates/
├── static/
├── requirements.txt
├── run.py
├── docker-compose.yml
└── README.md
```

---

## 📄 License

Licensed under the [MIT License](LICENSE).

---

## ❤️ Contribute

**MyBibliotheca** is open source and contributions are welcome!

Pull requests, bug reports, and feature suggestions are appreciated.
