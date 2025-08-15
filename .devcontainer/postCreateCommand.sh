git config devcontainers-theme.show-dirty 1

cat >> ~/.bashrc <<EOF
function pgmooncake_server() {
    sudo rm -rf /var/lib/postgresql
    sudo install -d -o vscode -g vscode /var/lib/postgresql
    mkdir -p /var/lib/postgresql/data
    docker run --name mooncake --rm -e POSTGRES_PASSWORD=password -v /var/lib/postgresql/data:/var/lib/postgresql/data --user "$(id -u):$(id -g)" mooncakelabs/pg_mooncake
}

function pgmooncake_client() {
    docker exec -it mooncake psql -U postgres
}
EOF
