#/bin/bash

# Conveniece script for reverting to arm64 mode (after building x86-64 images)
# on an Apple silicon mac when using colima instead of docker
# Set CPU, memory and disk according to needs

minikube stop
minikube delete
colima stop
colima delete -f
colima start --cpu 8 --memory 12 --disk 100
minikube start --cpus=8 --memory=11g --disk-size=95g