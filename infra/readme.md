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

Deploy watcher:

This runs the docker-compose app that watches the container registry. 
When a new release is made, the data collection app is gracefully closed and then the new version of the app is kickstarted.

```bash
mkdir -p /opt/at-collector && cd /opt/at-collector

# Create docker-compose.yml (copy from above infra/docker-compose.yml)
nano docker-compose.yml

# Create .env file (copy from .env. Replace `SENTRY_ENVIRONMENT` with Production)
nano .env

# Start services
docker compose up -d
```

# Re-start application

To restart the watcher application:

access the droplet console

```bash
# Move to directory
cd /opt/at-collector

# Run watcher
docker compose up -d
```


# Deployment Workflow

1. Make changes to code locally
2. Commit and push to GitHub
3. Create a release/tag. Github > Create Release > Create tag `v*.*.*`
4. GitHub Actions builds and pushes image to ghcr.io
5. Watchtower on VPS detects new image within 5 minutes
6. Container automatically restarts with new version


# Useful commands

```bash
# See watchtower logs
docker logs -f watchtower

# See AT Historical Data Collection logs
docker logs -f at-collector

# See container SHA for data collector. Match against SHA in container registry
docker inspect at-collector | grep Image

# See running containers
docker ps

# See container stats
docker stats
```



# Download Data for Analysis

From your local machine (via WSL on Windows):

```bash
rsync -avzP root@170.64.237.137:/opt/at-collector/data/ ~/at-data/
```
