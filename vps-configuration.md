# VPS Configuration Reference
## Server
| Item | Value |
|------|-------|
| Provider | Ionos |
| IP Address | 198.71.50.88 |
| OS | Ubuntu 24.04.4 LTS (Noble Numbat) |
| Plan | VPS M — 2 vCores, 4GB RAM, 120GB NVMe |

---

## Stack
| Component | Version | Notes |
|-----------|---------|-------|
| nginx | 1.24.0 | Reverse proxy, static file serving |
| PHP | 8.3 (FPM) | For cape-coder.info landing page |
| Python | 3.12 | System |
| Gunicorn | 25.3.0 | WSGI server for Flask apps |
| Flask | 3.1.3 | |
| SQLite | 3 | Application database |
| Certbot | — | Let's Encrypt SSL, auto-renewing |
| UFW | — | Firewall — OpenSSH + Nginx Full allowed |

---

## Users
| User | Role | Notes |
|------|------|-------|
| root | Admin | SSH access, server management |
| deployer | Deploy user | Runs Gunicorn, receives GitHub Actions deploys |

### deployer sudo permissions
```
/etc/sudoers.d/dclt-nav
deployer ALL=(ALL) NOPASSWD: /bin/systemctl restart dclt-nav
```

---

## SSH Keys
| Key | Location | Purpose |
|-----|----------|---------|
| `~/.ssh/ionos_vps` | Local Mac | Personal SSH access to VPS |
| `~/.ssh/ionos_vps.pub` | `/home/deployer/.ssh/authorized_keys` | Key-based auth for deployer |

### Mac SSH config (~/.ssh/config)
```
Host ionos-vps
    HostName 198.71.50.88
    User deployer
    IdentityFile ~/.ssh/ionos_vps
```

---

## Sites

### cape-coder.info
| Item | Value |
|------|-------|
| Type | Static PHP site |
| Web root | `/var/www/html` |
| nginx config | `/etc/nginx/sites-available/cape-coder.info` |
| SSL | `/etc/letsencrypt/live/cape-coder.info/` |
| Deploy | GitHub Actions → SFTP mirror on push to master |
| Repo | `github.com/Forddevstewart/cape-coder.info` |

**GitHub Actions secrets:**
- `SFTP_HOST` → `198.71.50.88`
- `SFTP_USERNAME` → `deployer`
- `SFTP_PASSWORD` → deployer password

### dclt-nav.cape-coder.info
| Item | Value |
|------|-------|
| Type | Flask + Gunicorn + SQLite |
| App root | `/var/www/dclt-nav` |
| venv | `/var/www/dclt-nav/.venv` |
| Socket | `/var/www/dclt-nav/dclt-nav.sock` |
| nginx config | `/etc/nginx/sites-available/dclt-nav` |
| SSL | `/etc/letsencrypt/live/dclt-nav.cape-coder.info/` |
| systemd service | `/etc/systemd/system/dclt-nav.service` |
| Logs | `/var/log/dclt-nav-access.log`, `/var/log/dclt-nav-error.log` |
| Deploy | GitHub Actions → SSH → git pull → restart on push to main |
| Repo | `github.com/Forddevstewart/dclt-nav` |

**GitHub Actions secrets:**
- `VPS_HOST` → `198.71.50.88`
- `VPS_USER` → `deployer`
- `VPS_SSH_KEY` → contents of `~/.ssh/ionos_vps`
- `VPS_SSH_PASSPHRASE` → ionos_vps key passphrase

---

## systemd Service — dclt-nav
```ini
[Unit]
Description=DCLT Navigator Flask App
After=network.target

[Service]
User=deployer
Group=www-data
WorkingDirectory=/var/www/dclt-nav
Environment="PATH=/var/www/dclt-nav/.venv/bin"
ExecStart=/var/www/dclt-nav/.venv/bin/gunicorn \
    --workers 3 \
    --bind unix:/var/www/dclt-nav/dclt-nav.sock \
    --access-logfile /var/log/dclt-nav-access.log \
    --error-logfile /var/log/dclt-nav-error.log \
    wsgi:application
Restart=always

[Install]
WantedBy=multi-user.target
```

---

## nginx Config — cape-coder.info
```nginx
server {
    listen 80;
    server_name cape-coder.info www.cape-coder.info;
    root /var/www/html;
    index index.php index.html;

    location / {
        try_files $uri $uri/ =404;
    }

    location ~ \.php$ {
        include snippets/fastcgi-php.conf;
        fastcgi_pass unix:/run/php/php8.3-fpm.sock;
    }

    location ~ /\.ht {
        deny all;
    }
}
```
*(Certbot appends SSL server block automatically)*

---

## nginx Config — dclt-nav
```nginx
server {
    listen 80;
    server_name dclt-nav.cape-coder.info;

    location / {
        proxy_pass http://unix:/var/www/dclt-nav/dclt-nav.sock;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```
*(Certbot appends SSL server block automatically)*

---

## DNS (Ionos — cape-coder.info)
| Type | Host | Value |
|------|------|-------|
| A | @ | 198.71.50.88 |
| A | www | 198.71.50.88 |
| A | dclt-nav | 198.71.50.88 |

Old shared hosting IP (for reference): `74.208.236.30`

---

## File Layout
```
/var/www/
  html/                    ← cape-coder.info web root
  dclt-nav/                ← DCLT Navigator app root
    .venv/                 ← Python virtualenv
    app/
      __init__.py
      routes.py
      api.py
      models.py
      templates/
      static/
        pdfs/              ← PDF documents served by nginx
    data/
      reference.db         ← built locally, pushed via rsync (read-only to Flask)
      transactional.db     ← born on server, never overwritten by rsync
    dclt-nav.sock          ← Gunicorn unix socket
    wsgi.py
    requirements.txt
    sync_to_vps.sh         ← run locally on Mac to push data

/var/log/
  dclt-nav-access.log
  dclt-nav-error.log

/etc/nginx/sites-available/
  cape-coder.info
  dclt-nav

/etc/systemd/system/
  dclt-nav.service

/etc/sudoers.d/
  dclt-nav
```

---

## Adding a New Project
1. Clone repo to `/var/www/projectname`
2. Create venv, install dependencies
3. Create systemd service at `/etc/systemd/system/projectname.service`
4. Create nginx server block at `/etc/nginx/sites-available/projectname`
5. Symlink to sites-enabled
6. Add DNS A record for subdomain → `198.71.50.88`
7. Run `certbot --nginx -d subdomain.cape-coder.info`
8. Add sudoers entry for systemctl restart
9. Add GitHub Actions secrets and deploy workflow

---

## Key Commands
```bash
# Restart a service
sudo systemctl restart dclt-nav

# Check service status
systemctl status dclt-nav

# View logs
tail -f /var/log/dclt-nav-error.log

# Test nginx config
nginx -t

# Reload nginx
systemctl reload nginx

# Renew SSL (runs automatically, manual if needed)
certbot renew
```