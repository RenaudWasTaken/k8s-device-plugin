// Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.

package main

import (
	"log"

	"github.com/NVIDIA/gpu-monitoring-tools/bindings/go/nvml"
)

func main() {
	log.Println("Loading NVML")
	if err := nvml.Init(); err != nil {
		log.Printf("Failed to initialize NVML: %s.", err)
		log.Printf("If this is a GPU node, did you set the docker default runtime to `nvidia`?")
		log.Printf("You can check the prerequisites at: https://github.com/NVIDIA/k8s-device-plugin#prerequisites")
		log.Printf("You can learn how to set the runtime at: https://github.com/NVIDIA/k8s-device-plugin#quick-start")

		select {}
	}
	defer func() { log.Println("Shutdown of NVML returned:", nvml.Shutdown()) }()

	log.Println("Fetching devices.")
	if len(getDevices()) == 0 {
		log.Println("No devices found. Waiting indefinitely.")
		select {}
	}

	devicePlugin := NewNvidiaDevicePlugin()
	if err := devicePlugin.Serve(); err != nil {
		log.Println("Could not contact Kubelet, retrying. Did you enable the device plugin feature gate?")
		log.Printf("You can check the prerequisites at: https://github.com/NVIDIA/k8s-device-plugin#prerequisites")
		log.Printf("You can learn how to set the runtime at: https://github.com/NVIDIA/k8s-device-plugin#quick-start")
	}

	for {
	}
}
