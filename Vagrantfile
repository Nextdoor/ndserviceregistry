Vagrant.configure("2") do |config|
  config.vm.hostname = "zookeeper"

  config.vm.box = "Berkshelf-CentOS-6.3-x86_64-minimal"
  config.vm.box_url = "https://dl.dropbox.com/u/31081437/Berkshelf-CentOS-6.3-x86_64-minimal.box"

  config.vm.network :private_network, ip: "33.33.33.100"
  config.vm.network :forwarded_port, guest: 2182, host: 2182
  config.vm.network :forwarded_port, guest: 8080, host: 8080

  # Assign this VM to a bridged network, allowing you to connect directly to a
  # network using the host's network device. This makes the VM appear as another
  # physical device on your network.

  # config.vm.network :bridged

  # Forward a port from the guest to the host, which allows for outside
  # computers to access the VM, whereas host only networking does not.
  # config.vm.forward_port 80, 8080

  # Share an additional folder to the guest VM. The first argument is
  # an identifier, the second is the path on the guest to mount the
  # folder, and the third is the path on the host to the actual folder.
  # config.vm.share_folder "v-data", "/vagrant_data", "../data"

  config.ssh.max_tries = 40
  config.ssh.timeout   = 120

  config.vm.provision :shell,
    inline: "yum -y install patch"

  config.vm.provision :chef_solo do |chef|
    chef.json = {
      "exhibitor" => {
        "defaultconfig" => {
          "client_port" => "2182"
        }
      }
    }

    chef.run_list = [
      "recipe[zookeeper]"
    ]
  end
end
