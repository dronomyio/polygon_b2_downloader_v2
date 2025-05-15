######################################################


  580  docker-compose build
  581  docker-compose run --rm discoverer discoverer historical --start_date 2025-05-08 --end_date 2025-05-09
  582  docker network ls
  583  docker run --rm --network polygon_b2_downloader_v2_default byrnedo/alpine-curl -v https://files.polygon.io
  584  sudo ufw status verbose
  585  sudo ufw allow out to 198.44.194.17 port 443 proto tcp
  586  sudo iptables -L -v -n
  587  iptables -t nat -L
  588  sudo iptables -t nat -L
  589  sudo iptables -t nat -L -v -n
  590  sysctl net.ipv4.ip_forward
  591  sudo vi /etc/sysctl.conf 
  592  sudo sysctl -p
  593  sudo systemctl restart docker
  594  docker run --rm --network polygon_b2_downloader_v2_default byrnedo/alpine-curl -v https://files.polygon.io
  595  docker-compose down
  596  docker network rm polygon_b2_downloader_v2_default
  597  docker run --rm --network polygon_b2_downloader_v2_default byrnedo/alpine-curl -v https://files.polygon.io
  598  sudo iptables -L -v -n
  599  sudo journalctl -u docker.service
  600  sudo vi /etc/docker/daemon.json
  601  bash     docker network ls
  602  bash     docker network ls
  603  bash     docker-compose run --rm discoverer echo "Recreating network"  
  604  docker-compose run
  605  bash     docker-compose run --rm discoverer echo "Recreating network"  
  606  bash     docker network ls
  607  file /usr/bin/docker
  608  ls -l /usr/bin/docker
  609  file /usr/local/bin/docker-compose
  610  ls -l /usr/local/bin/docker-compose
  611  docker --version
  612  docker-compose --version
  613  docker-compose down
  614  docker-compose run --rm discoverer echo "Recreating network"
  615  docker network ls
  616  docker run --rm --network polygon_b2_downloader_v2_default byrnedo/alpine-curl -v https://files.polygon.io
  617  sudo iptables -L -v -n
  618  sudo iptables -t nat -L -v -n
  619  sudo iptables -t nat -A POSTROUTING -s 172.18.0.0/16 ! -o docker0 -j MASQUERADE
  620  sudo iptables -t nat -A POSTROUTING -s 172.18.0.0/16 -j MASQUERADE
  621  sudo iptables -I DOCKER-USER -s 172.18.0.0/16 -j ACCEPT
  622  sudo iptables -I DOCKER-USER -d 172.18.0.0/16 -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
  623  docker run --rm --network polygon_b2_downloader_v2_default byrnedo/alpine-curl -v https://files.polygon.io
  624  docker-compose run --rm discoverer discoverer historical --start_date 2025-05-08 --end_date 2025-05-09
  625  docker-compose run --rm worker
  626  ls
  627  history
minasm@lambda-quad:~/suvasis/tools/blogs/polygon-backblaze/polygon_b2_downloader_v2$ 




##########################################################
## Resolving Docker Network Connectivity Issues to External Services

This document outlines the troubleshooting steps taken to resolve an issue where Docker containers were unable to connect to external services (specifically `https://files.polygon.io`), resulting in `ConnectTimeoutError`.

### Problem Description

Docker containers running within a custom `docker-compose` network were consistently failing to establish connections to external URLs, such as `https://files.polygon.io`. The application logs showed `ConnectTimeoutError`, and direct `curl` tests from within the affected containers to the target URL also timed out.

### Initial Symptoms

1.  Application logs indicated `ConnectTimeoutError` when attempting to reach Polygon.io S3.
2.  Manual `curl -v https://files.polygon.io` commands executed from within the Docker containers (using `docker run --network ... byrnedo/alpine-curl ...`) also resulted in connection timeouts, after successfully resolving the DNS.

### Troubleshooting Steps and Findings

The following steps were taken to diagnose the issue:

1.  **Application-Level Timeouts:** Confirmed that the application itself had reasonable connection and read timeouts for its S3 client. This was an early check and not the root cause of the persistent connection failure.

2.  **Host Firewall (UFW):** The user confirmed that UFW (Uncomplicated Firewall) on the host machine was `inactive`.

3.  **Host `iptables filter` Table Review:**
    *   The `OUTPUT` chain policy was `ACCEPT`.
    *   The `FORWARD` chain policy was `DROP` (a common default). Docker is expected to insert its own rules to allow traffic for its managed networks.
    *   No obvious host-level `iptables` rules were found that would block outbound traffic from the host itself.

4.  **Kernel IP Forwarding:** The user verified that IP forwarding was enabled on the host:
    ```bash
    sysctl net.ipv4.ip_forward
    # Output: net.ipv4.ip_forward = 1
    ```

5.  **Docker Service Restart:** The Docker service was restarted, but this did not resolve the issue on its own.

6.  **Docker Executable Check:** A temporary issue where `docker` and `docker-compose` binaries were reported as `cannot execute binary file` was resolved. `docker --version` and `docker-compose --version` confirmed the tools were operational.

7.  **Focus on Docker Network-Specific `iptables` Rules:**
    *   The Docker network used by `docker-compose` was identified (e.g., `polygon_b2_downloader_v2_default`).
    *   The subnet for this network was identified (e.g., `172.18.0.0/16`, with the test container getting an IP like `172.18.0.2`).
    *   **Key Finding 1 (NAT Table):** Examination of the `iptables -t nat -L -v -n` output revealed that while `MASQUERADE` rules existed for other Docker networks (e.g., `172.17.0.0/16`, `172.20.0.0/16`), a `MASQUERADE` rule was **missing** for the `172.18.0.0/16` subnet used by the application's network.
    *   **Key Finding 2 (Filter Table):** Similarly, the `iptables -L -v -n` output for the `filter` table did not show explicit `ACCEPT` rules in the `DOCKER-FORWARD` or `DOCKER-USER` chains that would allow traffic from the `172.18.0.0/16` subnet.

8.  **Docker Network Recreation Attempt:** The custom Docker network was removed (`docker-compose down`) and then recreated by running a `docker-compose` command (`docker-compose run --rm discoverer echo "Recreating network"`). However, Docker still failed to automatically generate the correct `iptables` rules for this newly recreated network.

### Resolution: Manually Adding `iptables` Rules

Since Docker was not automatically creating the necessary `iptables` rules for the `172.18.0.0/16` network, these rules were added manually to the host machine:

1.  **Added MASQUERADE Rule (for the `nat` table):** This rule allows containers on the `172.18.0.0/16` network to use the host's IP address for outbound connections (Network Address Translation).
    ```bash
    sudo iptables -t nat -A POSTROUTING -s 172.18.0.0/16 -j MASQUERADE
    ```

2.  **Added ACCEPT Rules (for the `filter` table, in the `DOCKER-USER` chain):** These rules explicitly allow traffic to be forwarded from the `172.18.0.0/16` network and allow related/established connections back to it.
    ```bash
    sudo iptables -I DOCKER-USER -s 172.18.0.0/16 -j ACCEPT
    sudo iptables -I DOCKER-USER -d 172.18.0.0/16 -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
    ```

### Outcome

After applying these manual `iptables` rules:

*   The `curl -v https://files.polygon.io` test executed from within a container on the `polygon_b2_downloader_v2_default` (172.18.0.0/16) network succeeded. It established a TLS connection and received an HTTP/2 403 response (which is expected for an unauthenticated request to the S3 endpoint base URL).
*   The main application (`polygon_b2_downloader_v2`) was then able to connect to Polygon.io S3 and successfully list files.

### Root Cause Summary

The root cause of the connectivity issue was that the Docker daemon on the user's specific system was failing to automatically generate the required `iptables` `FORWARD` (ACCEPT) and `nat` `POSTROUTING` (MASQUERADE) rules for the custom Docker Compose network operating on the `172.18.0.0/16` subnet.

### Note on Persistence

The manually added `iptables` rules may not persist across system reboots or Docker service restarts. If Docker continues to fail to create these rules automatically, further investigation into the Docker daemon's behavior on the host system would be needed. This could involve:

*   Checking Docker daemon logs for errors related to `iptables` rule creation (`sudo journalctl -u docker.service`).
*   Ensuring the Docker daemon configuration (`/etc/docker/daemon.json`) has `"iptables": true` (which is the default).
*   Investigating potential conflicts with other firewall management tools (though UFW was inactive in this case).

