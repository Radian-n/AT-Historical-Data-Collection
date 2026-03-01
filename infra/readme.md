# Setup DigitalOcean Droplet VPS

VPS: 1 vCPU, 1GB RAM, Ubuntu 24 LTS

On DigitalOcean UI:

Enable SSH key authentication.
Configure firewall (allow SSH only)

On VPS:

Install docker
```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# Log out and back in
```

Deploy Initial Application:

```bash
mkdir -p /opt/at-collector && cd /opt/at-collector

# Create docker-compose.yml (copy from above infra/docker-compose.yml)
nano docker-compose.yml

# Create .env file (copy from .env. Replace `SENTRY_ENVIRONMENT` with Production)
nano .env

# Start services
docker compose up -d
```

# Deployment Workflow

1. Make changes to code locally
2. Commit and push to GitHub
3. Create a release/tag: `git tag v1.0.0 && git push --tags`
4. GitHub Actions builds and pushes image to ghcr.io
5. Watchtower on VPS detects new image within 5 minutes
6. Container automatically restarts with new version


# Download Data for Analysis
From your local machine (via WSL on Windows):
```bash
rsync -avzP user@vps-ip:/opt/at-collector/data/ ~/at-data/
```