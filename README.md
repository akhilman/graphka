# graphka

Dataflow graph framework for Tarantool

## Commands
* `make dep` - Installs dependencies to ./.rocks folder
* `make run` - Runs Tarantool instance locally inside the ./.tnt/init folder.
* `make test` - Runs tests from ./t folder

## Deploy
To deploy application the recommended directory structure is the following:
```
/
├── etc
│   └── graphka
│       └── conf.lua
└── usr
    └── share
        └── graphka
            ├── init.lua
            ├── app/
            └── .rocks/
```
You need to put a symlink `/etc/tarantool/instances.enabled/graphka.lua -> /usr/share/graphka/init.lua
` and you are ready to start your application by either `tarantoolctl start graphka` or, if you're using systemd - `systemctl start tarantool@graphka`