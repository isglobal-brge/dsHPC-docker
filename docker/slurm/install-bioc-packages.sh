#!/bin/bash
set -e

# Get the default R library path cleanly
LIB_PATH=$(R -s -e "cat(.libPaths()[1])")

# Read packages from the file into an array
mapfile -t packages < /tmp/r_packages.txt

# Loop through the packages and install them one by one using BiocManager
for pkg in "${packages[@]}"; do
    echo "Installing R package: $pkg into ${LIB_PATH}"
    R -s -e "BiocManager::install('$pkg', lib='${LIB_PATH}', update=FALSE, ask=FALSE)"
    if [ $? -ne 0 ]; then
        echo "Failed to install R package: $pkg"
        exit 1
    fi
done

echo "All R packages installed successfully." 