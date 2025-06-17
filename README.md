# 📚 Bibliotheca

**Bibliotheca** is a self-hosted personal library and reading tracker web app built with Flask. It lets you log, organize, and visualize your reading journey. Add books by ISBN, track reading progress, log daily reading, and generate monthly wrap-up images of your finished titles.

🆕 **Multi-User Features**: Multi-user authentication, user data isolation, admin management, and secure password handling.

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

Bibliotheca can be run completely in Docker — no need to install Python or dependencies on your machine.

#### ✅ Prerequisites

- [Docker](https://www.docker.com/) installed
- [Docker Compose](https://docs.docker.com/compose/) installed

---

#### 🔁 Option 1: One-liner (Docker only)

```bash
docker run -d \
  -v bibliotheca_data:/app/data \
  -p 5054:5054 \
  --name bibliotheca \
  pickles4evaaaa/bibliotheca:latest
````

---

#### 🔁 Option 2: Docker Compose

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  bibliotheca:
    image: pickles4evaaaa/bibliotheca:latest
    container_name: bibliotheca
    ports:
      - "5053:5054"
    volumes:
      - bibliotheca_data:/app/data
    restart: unless-stopped
    environment:
      - TIMEZONE=America/Chicago  # ✅ Set your preferred timezone here
      - WORKERS=6  # Set the number of Gunicorn workers to 6

volumes:
  bibliotheca_data:

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
| `ADMIN_EMAIL`         | Default admin email                        | `admin@bibliotheca.local` |
| `ADMIN_USERNAME`      | Default admin username                     | `admin`                   |
| `ADMIN_PASSWORD`      | Default admin password                     | `changeme123`             |
| `TIMEZONE`            | Sets the app's timezone                    | `America/Chicago`         |
| `READING_STREAK_OFFSET` | Adjusts reading day streak | `160` (160 days + new days logged)    |
| `WORKERS`             | Number of Gunicorn worker processes        | `6`                      |

---

## 🔐 Authentication & User Management

### First Time Setup

Bibliotheca automatically creates an admin user during first run:
- **Username**: `admin` (customizable via `ADMIN_USERNAME`)
- **Email**: `admin@bibliotheca.local` (customizable via `ADMIN_EMAIL`)
- **Password**: `changeme123` (customizable via `ADMIN_PASSWORD`)

⚠️ **Important**: Change the default admin password after first login!

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
- All existing books are assigned to the admin user  
- No data is lost during migration
- V1.x functionality remains unchanged
- **No manual steps required** - just start the application!

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
   git clone https://github.com/pickles4evaaaa/bibliotheca.git
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

4. **Run the app**

   ```bash
   gunicorn -w NUMBER_OF_WORKERS -b 0.0.0.0:5054 run:app
   ```

   Visit: [http://127.0.0.1:5054](http://127.0.0.1:5054)

> 💡 No need to manually set up the database — it is created automatically on first run.

---

### ⚙️ Configuration

* By default, uses SQLite (`books.db`) and a simple dev secret key.
* For production, you can configure:

  * `SECRET_KEY`
  * `DATABASE_URI`
    via environment variables or `.env`.

---

## 🗂️ Project Structure

```
bibliotheca/
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

**Bibliotheca** is open source and contributions are welcome!

Pull requests, bug reports, and feature suggestions are appreciated.