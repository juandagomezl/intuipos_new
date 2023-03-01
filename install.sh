curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list


apt update
apt install python3-pip unixodbc-dev -y
ACCEPT_EULA=Y apt install -y msodbcsql17
