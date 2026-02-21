#!/bin/bash
set -e

echo "========================================="
echo "Container started successfully!"
echo "========================================="
echo ""
echo "Available tools:"
echo "  - AWS CLI: $(aws --version 2>&1 | head -1)"
echo "  - MongoDB: $(mongod --version | head -1)"
echo "  - s2d: $(which s2d)"
echo ""
echo "Environment:"
echo "  - OS: $(cat /etc/redhat-release)"
echo ""

# Add your custom logic here
if [ -n "$1" ]; then
    echo "Running custom command: $@"
    exec "$@"
else
    echo "No command provided. Exiting..."
fi
