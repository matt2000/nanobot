FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Install Node.js 20 for the WhatsApp bridge:
#    mkdir -p /etc/apt/keyrings && \
#    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
#    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" > /etc/apt/sources.list.d/nodesource.list && \
#    apt-get update && \
#    apt-get install -y --no-install-recommends nodejs && \
#    apt-get purge -y gnupg && \

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates gnupg git && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (cached layer)
COPY pyproject.toml ./
RUN mkdir -p nanobot bridge && touch nanobot/__init__.py && \
    uv pip install --system --no-cache . && \
    rm -rf nanobot bridge

# Install signal-cli
RUN /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
RUN echo >> /root/.bashrc && \
    echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv bash)"' >> /root/.bashrc && \
    eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv bash)"
RUN /home/linuxbrew/.linuxbrew/bin/brew install signal-cli
COPY signal-cli-config/ /root/.local/share/signal-cli/


# Copy the full source and install
COPY nanobot/ nanobot/
COPY bridge/ bridge/
RUN uv pip install --system --no-cache .

# Build the WhatsApp bridge
#WORKDIR /app/bridge
#RUN npm install && npm run build
#WORKDIR /app

# Create config directory
RUN mkdir -p /root/.nanobot

# Gateway default port
EXPOSE 18790

ENV BASH_ENV=/root/.bashrc
CMD /bin/bash -c "nanobot gateway"
