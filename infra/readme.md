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

# Create .env file
# Fill out and copy the contents of `infra/.example.env`
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


# Deployment Workflow

1. Make changes to code locally
2. Commit and push to GitHub
3. Create a release/tag. Github > Create Release > Create tag `v*.*.*`
4. GitHub Actions builds and pushes image to ghcr.io
5. Watchtower on VPS detects new image within 5 minutes
6. Container automatically restarts with new version


# R2

To delete an R2 bucket's contents. On your local machine, install rclone, and 
configure the remote repository.

Then run the following to delete a bucket's contents:

```bash
rclone delete r2:test-at-gtfs-data
```


# Access Data for Analysis (from anywhere)
```python
# On your local machine - no VPS access needed!
from deltalake import DeltaTable

storage_options = {
    "AWS_ACCESS_KEY_ID": "your_access_key",
    "AWS_SECRET_ACCESS_KEY": "your_secret_key",
    "AWS_ENDPOINT_URL": "https://{account_id}.r2.cloudflarestorage.com",
    "AWS_REGION": "auto",
}

dt = DeltaTable(
    "s3://at-gtfs-data/processed/vehicle_positions",
    storage_options=storage_options
)
df = dt.to_pandas()

# Or with Polars
import polars as pl
df = pl.read_delta(
    "s3://at-gtfs-data/processed/vehicle_positions",
    storage_options=storage_options
)
```

