#/bin/bash

# Conveniece script when building x86-64 images on an Apple silicon mac
# when using colima instead of docker

minikube stop
minikube delete
colima stop
colima delete -f
colima start --arch x86_64 --cpu 2 --memory 4 --disk 20
minikube start --cpus=2 --memory=3g --disk-size=15g