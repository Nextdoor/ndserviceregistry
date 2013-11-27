Vagrant.configure("2") do |config|
  config.vm.hostname = "zookeeper"

  config.vm.box = "Berkshelf-CentOS-6.3-x86_64-minimal"
  config.vm.box_url = "https://dl.dropbox.com/u/31081437/Berkshelf-CentOS-6.3-x86_64-minimal.box"

  config.vm.network :private_network, ip: "33.33.33.100"
  config.vm.network :forwarded_port, guest: 2182, host: 2182
  config.vm.network :forwarded_port, guest: 8080, host: 8080

  config.vm.provision :shell,
    inline: <<SCRIPT
  yum -y install patch
  mkdir -p /var/chef/cache
SCRIPT

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
