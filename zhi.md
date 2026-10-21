## 查看cpu占用
~~~ 
# 按 CPU 排序看进程
ps -eo user,%cpu,%mem,cmd --sort=-%cpu | head -30

# 按用户汇总 CPU
ps -eo user,%cpu --no-headers | awk '{cpu[$1]+=$2} END {for (u in cpu) print u, cpu[u]}' | sort -k2 -nr

# 查找真实用户名
cut -d: -f1 /etc/passwd | grep yangdin

# 查看某个用户进程
ps -u yangdingli -o pid,cmd,%cpu --sort=-%cpu
ps -u lyuce -o pid,cmd,%cpu --sort=-%cpu
ps -u lwm -o pid,cmd,%cpu --sort=-%cpu
ps -u root -o pid,cmd,%cpu --sort=-%cpu
ps -u systemd-coredump -o pid,cmd,%cpu --sort=-%cpu | head

# 查看进程详情
ps -o pid,nlwp,cmd -p 1433234

# 查看线程 CPU
top -Hp 1433234

~~~

## 排查登录缓慢

~~~

#/dev/mapper/ubuntu--vg-ubuntu--lv 是你的系统根盘。
#/usr/local/games/.mozilla/.mozilla 是异常/可疑目录，很可能是某个沙箱或浏览器伪装环境留下的挂载点。
#真正的问题是根盘被重复挂载到这些位置 3.8 万次，导致 df、loginctl、SSH 登录变慢。

uptime
top -bn1 | head -20
ps -eo pid,ppid,stat,user,%cpu,%mem,etime,comm,args --sort=-%cpu | head -30
ps -eo pid,ppid,stat,user,%cpu,%mem,etime,comm,args --sort=-%mem | head -30
df -h

# 发现df -h 卡住，某个文件系统/挂载点在卡。
timeout 5s df -h /
timeout 5s df -h -l
ps -eo pid,ppid,stat,wchan:32,user,comm,args | awk '$3 ~ /D/ {print}'
vmstat 1 5
cat /proc/mounts | awk '{print $2, $3}' | column -t

# 机器不是完全满载，但有很多 runnable 进程。更像是：命令卡在某个挂载点/命名空间/系统调用上，但不是 D 状态表现
# df 被某些挂载信息拖住，/proc/mounts 里挂载项非常多，df 遍历时慢

#找出超时的挂载点
cat /proc/mounts | awk '{print $2}' | while read m; do
  timeout 2s df -h "$m" >/dev/null 2>&1 || echo "TIMEOUT $m"
done | head -30


# 看挂载数量
wc -l /proc/mounts

# 查 Codex/沙箱/tmp 是否很多
find ~/.codex/tmp -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l
du -sh ~/.codex/tmp 2>/dev/null
du -sh ~/.codex/tmp 2>/dev/null
find /tmp -mindepth 1 -maxdepth 1 -user "$USER" 2>/dev/null | wc -l
find ~/.codex/tmp -mindepth 1 -maxdepth 1 -type d -mtime +1 2>/dev/null | wc -l
# 如果 ~/.codex/tmp 目录很多，可以清理旧的。先看 1 天前的数量：
find ~/.codex/tmp -mindepth 1 -maxdepth 1 -type d -mtime +1 2>/dev/null | wc -l

# 核心问题38199 ext4           38260 /proc/mounts 正常服务器一般是几十个挂载点，复杂一点也就几百个。你这里有 三万八千多个 ext4 挂载点，df -h 卡住基本就是因为它要遍历这 38260 个挂载点。

# 找这些 38199 个 ext4 挂载在哪里
awk '$3=="ext4"{print $2}' /proc/mounts | sed 's#/[^/]*$##' | sort | uniq -c | sort -nr | head -50
# 再看前 50 个具体挂载点：
awk '$3=="ext4"{print $2}' /proc/mounts | head -50
# 看后 50 个：
awk '$3=="ext4"{print $2}' /proc/mounts | tail -50
# 看是不是重复挂载同一个设备
awk '$3=="ext4"{print $1}' /proc/mounts | sort | uniq -c | sort -nr | head -30

# 挂载泄漏 38197 /dev/mapper/ubuntu--vg-ubuntu--lv
# 先看可疑挂载数量：
awk '$3=="ext4" && ($2 ~ "^/proc/[0-9]+$" || $2 == "/usr/local/games/.mozilla/.mozilla") {print $2}' /proc/mounts | wc -l
# 再看具体前几项：
awk '$3=="ext4" && ($2 ~ "^/proc/[0-9]+$" || $2 == "/usr/local/games/.mozilla/.mozilla") {print $2}' /proc/mounts | head -30
# 确认只包含：/proc/数字        /usr/local/games/.mozilla/.mozilla

#先备份 root crontab：
sudo crontab -l > /tmp/root.cron.before.cleanup.$(date +%Y%m%d-%H%M%S)
#删除那一行：
sudo crontab -l | grep -vF '/usr/local/games/.mozilla/.mozilla' | sudo crontab -
#确认已经删掉：
sudo crontab -l | grep -nF '/usr/local/games/.mozilla' || echo "root crontab clean"
#如果输出 root crontab clean 就对了。
#刚才 ps 没看到匹配进程，但因为 cron 每分钟会拉起，建议再杀一次精确路径：
sudo pkill -f '/usr/local/games/.mozilla/.mozilla' || true
#然后确认：
ps -ef | grep -Ei '/usr/local/games|\.mozilla|run64|h64

sudo cat /usr/local/games/.mozilla/.mozilla/pid 2>/dev/null
sudo cat /usr/local/games/.mozilla/.mozilla/cron 2>/dev/null
sudo file /usr/local/games/.mozilla/.mozilla/*
ps -ef | grep -Ei '/usr/local/games|\.mozilla|run64|h64|update' | grep -v grep
# 如果确认没有正在运行的恶意进程，再卸载异常挂载：
awk '$3=="ext4" && ($2 ~ "^/proc/[0-9]+$" || $2 == "/usr/local/games/.mozilla/.mozilla") {print $2}' /proc/mounts \
  | sort -r \
  | sudo xargs -r -n1 umount -l

# 执行完后检查挂载数量：
wc -l /proc/mounts
# 再看 ext4 数量：
cat /proc/mounts | awk '{cnt[$3]++} END {for (t in cnt) print cnt[t], t}' | sort -nr | head -30

最后删：
sudo rm -rf /usr/local/games/.mozilla
~~~

## 开代理

~~~
# 验证 mihomo 是否长期运行，显示：active (running)
systemctl --user status mihomo

# 退出 SSH，lwm 的 user systemd 服务也继续运行
sudo loginctl enable-linger lwm
# 检查 linger：Linger=yes
loginctl show-user lwm | grep Linger

# 显式指定代理环境变量或代理参数
ALL_PROXY=socks5h://127.0.0.1:7891 你的命令

# 不想每条命令都写，可以先在当前终端里开启：
export ALL_PROXY=socks5h://127.0.0.1:7891
export all_proxy=socks5h://127.0.0.1:7891
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890
export http_proxy=http://127.0.0.1:7890
export https_proxy=http://127.0.0.1:7890
运行指令
# 关闭代理：
unset ALL_PROXY all_proxy HTTP_PROXY HTTPS_PROXY http_proxy https_proxy

# 全局设置~/.bashrc
alias proxy_on='export ALL_PROXY=socks5h://127.0.0.1:7891 HTTP_PROXY=http://127.0.0.1:7890 HTTPS_PROXY=http://127.0.0.1:7890 all_proxy=socks5h://127.0.0.1:7891 http_proxy=http://127.0.0.1:7890 https_proxy=http://127.0.0.1:7890'
alias proxy_off='unset ALL_PROXY HTTP_PROXY HTTPS_PROXY all_proxy http_proxy https_proxy'

source ~/.bashrc

#使用时 
proxy_on
# 验证：返回香港/日本/美国等节点 IP
curl https://ipinfo.io 
# 不用代理：
proxy_off
~~~

## 创建 tmux 会话

~~~
tmux new -s tlp_cov
tmux new -s dqp_cov
~~~

## 新开 window：

~~~
Ctrl + b  然后 c
~~~

## 查看 window：

~~~
Ctrl + b
然后按 w

Ctrl + b
数字跳转
~~~

## 回到 tmux

~~~
tmux ls
tmux attach -t tidb_cov
~~~

## 在tumx离开

~~~
Ctrl + b
然后按 d

~~~

## 清理 tmux session

~~~
tmux ls

tmux kill-session -t tidb_cov
tmux kill-session -t tlp_cov

## 如果有多个
tmux kill-server
~~~

## 找TiDB进程

~~~·
ps aux | grep tidb-server
~~~

## 杀掉进程

~~~
kill -9 PID
~~~

## 看端口

~~~
# 用这个命令看所有监听端口：
ss -lntp
# 只检查某个端口，比如 4003：
ss -lntp | grep ':4003'
~~~

## 编译 coverage 版 TiDB：编译阶段不会污染，只是在代码里埋计数器，不会记录历史 coverage。运行阶段如果共目录会污染

~~~
cd ~/tidb

go build \
  -cover \
  -coverpkg=./... \
  -o bin/tidb-server-cov \
  ./cmd/tidb-server

ls bin

cd /home/lwm/tidb

env GOPATH=/home/lwm/go PATH=/usr/local/go/bin:/usr/bin:/bin:/home/lwm/go/bin \
CGO_ENABLED=1 GO111MODULE=on \
go build \
  -cover \
  -covermode=atomic \
  -tags codes \
  -ldflags '-X "github.com/pingcap/tidb/pkg/parser/mysql.TiDBReleaseVersion=v8.5.5" -X "github.com/pingcap/tidb/pkg/util/versioninfo.TiDBGitHash=1fa258b833ff113883beeba40bc130be7ce66610" -X "github.com/pingcap/tidb/pkg/util/versioninfo.TiDBGitBranch=HEAD" -X "github.com/pingcap/tidb/pkg/util/versioninfo.TiDBEdition=Community"' \
  -o bin/tidb-server-cov-real \
  ./cmd/tidb-server
~~~


## 在 tmux 里启动 TiDB

~~~
rm -rf ~/coverage/tlp
rm -rf ~/tidb-data/tlp
mkdir -p ~/coverage/tlp
mkdir -p ~/tidb-data/tlp

cd ~/tidb
GOCOVERDIR=~/coverage/tlp \
./bin/tidb-server-cov \
  --store=unistore \
  --path=/home/lwm/tidb-data/tlp \
  -P 4003 \
  -status 10083 
~~~
GOCOVERDIR=~/coverage/lc69 ./bin/tidb-server-cov-real   --store=unistore   --path=/home/lwm/tidb-data/lc69   -P 4003   -status 10083

GOCOVERDIR=~/coverage/cert612 \
./bin/tidb-server-cov \
  --store=unistore \
  --path=/home/lwm/tidb-data/tlp \
  -P 4003 \
  -status 10083 

~~~
rm -rf ~/coverage/dqp
rm -rf ~/tidb-data/dqp
mkdir -p ~/coverage/dqp
mkdir -p ~/tidb-data/dqp

cd ~/tidb
GOCOVERDIR=~/coverage/dqp \
./bin/tidb-server-cov \
  --store=unistore \
  --path=/home/lwm/tidb-data/dqp \
  -P 4004 \
  -status 10084

rm -rf ~/coverage/lc68
rm -rf ~/tidb-data/lc68
mkdir -p ~/tidb-data/lc611
mkdir -p ~/coverage/lc611

cd ~/tidb
GOCOVERDIR=~/coverage/lc611 ./bin/tidb-server-cov-real     --store=unistore     --path=/home/lwm/tidb-data/lc611     -P 4005     -status 10085
~~~

## 验证两个实例活着

~~~
ss -lntp | grep tidb-server
~~~

## SQLancer覆盖率（12h）

~~~
cd ~/lc/sqlancer

timeout 12h java -jar target/sqlancer-*.jar \
  --host 127.0.0.1 \
  --port 4003 \
  --username root \
  --password "" \
  tidb \
  --oracle QUERY_PARTITIONING \
  | tee ~/coverage/tlp/logs/sqlancer.log
~~~

~~~
cd ~/lc/sqlancer

timeout 12h java -jar target/sqlancer-*.jar \
  --host 127.0.0.1 \
  --port 4004 \
  --username root \
  --password "" \
  tidb \
  --oracle DQP \
  | tee ~/coverage/dqp/logs/sqlancer.log
~~~

## 检查 coverage 文件有没有生成

~~~
ls ~/coverage/dqp | head
~~~


## 检查 coverage 是否成功

~~~
ls ~/coverage/tlp
ls ~/coverage/dqp
#能看到
#covmeta.*
#covcounters.*
~~~

# 验证文件存在

~~~
ls -lh ~/coverage/tlp.out

go tool cover -func=/home/lwm/coverage/tlp.out | tail -1
~~~

## 查看覆盖率数据

~~~
cd ~/tidb

mkdir -p ~/coverage/tlp_merged

go tool covdata merge \
  -i ~/coverage/tlp \
  -o ~/coverage/tlp_merged

go tool covdata textfmt \
  -i ~/coverage/tlp_merged \
  -o ~/coverage/tlp.out

ls -lh ~/coverage/tlp.out     
go tool cover -func=/home/lwm/coverage/tlp.out | tail -1
~~~

~~~
cd tidb
mkdir -p ~/coverage/dqp_merged

go tool covdata merge \
  -i ~/coverage/dqp \
  -o ~/coverage/dqp_merged

go tool covdata textfmt \
  -i ~/coverage/dqp_merged \
  -o ~/coverage/dqp.out          

ls -lh ~/coverage/dqp.out     
go tool cover -func=/home/lwm/coverage/dqp.out | tail -1
~~~

~~~
cd tidb
mkdir -p ~/coverage/lc611_merged

go tool covdata merge   -i ~/coverage/lc611   -o ~/coverage/lc611_merged
go tool covdata textfmt   -i ~/coverage/lc611_merged   -o ~/coverage/lc611.out
go tool cover -func=/home/lwm/coverage/lc611.out | tail -1
~~~

## 暴露端口给师兄测试覆盖率

~~~
# 检查 4005 端口是否已被占用
ss -lntp | grep 4005

mkdir -p ~/coverage/lc68
mkdir -p ~/tidb-data/lc68

GOCOVERDIR=~/coverage/lc68 \
./bin/tidb-server-cov-real \
    --store=unistore \
    --path=/home/lwm/tidb-data/lc68 \
    -P 4005 \
    -status 10085

# 注意10085不要跟其他正在运行的重复
~~~

## 修改sqlancer参数

~~~
#src/sqlancer/MainOptions.java文件

cd ~/sqlancer
mvn clean package -Dmaven.test.skip=true
ls target/*.jar
~~~

## tidb混合存储实验

~~~
cd tidb
tiup playground --tiflash 1 --db.port 4010 --without-monitor
# 连接你的 TiDB 实例（端口 4010）
mysql -h 127.0.0.1 -P 4010 -u root

cd sqlancer
mvn -q -DskipTests package

timeout 12h java -jar target/sqlancer-2.0.0.jar \
  --num-threads 2 \
  --host 127.0.0.1 \
  --port 4010 \
  --username root \
  --password "" \
  tidb \
  --oracle MIXED_STORAGE \
  --storage-config MIXED
~~~

## docker启动mysql

~~~
ss -tlnp | grep 3307

sudo docker run -d \
  --name mysql-lwm \
  -e MYSQL_ROOT_PASSWORD=123456 \
  -p 3307:3306 \
  mysql:8.0

docker logs -f mysql-lwm

mysql -h 127.0.0.1 -P 3307 -u root -p123456
~~~





## 清理磁盘空间

~~~
df -h

sudo ncdu -x /

sudo du -sh /home/*

# 看 Docker 总体占用：
docker system df -v

# 查某个 volume 挂载到哪些容器
sudo docker ps -a --filter volume=4e67aecd48a0f361b814eaba1f6760c92675bdc09f0fb792d7c6a0c8aa470cb5

# 找到数据存放路径"Mountpoint": "/var/lib/docker/volumes/4e67.../_data"
sudo docker volume inspect 4e67aecd48a0f361b814eaba1f6760c92675bdc09f0fb792d7c6a0c8aa470cb5

# 停止容器
sudo docker stop clickhouse267-lwm726

# 清空数据
# sudo rm -rf /var/lib/docker/volumes/ae62e82ef690caf997ef97ec4521888b8066a018244ae5127382604ea82c3b54/_data
sudo find /var/lib/docker/volumes/18c82294a28cbc55daba3e896b330c410fc25e5da14172fb0416eec7c61307ad/_data \
  -mindepth 1 -maxdepth 1 -exec rm -rf -- "{}" +

# 重新创建 _data 目录
# sudo mkdir -p /var/lib/docker/volumes/18c82294a28cbc55daba3e896b330c410fc25e5da14172fb0416eec7c61307ad/_data

# 修复权限
# sudo chown -R 101:101 /var/lib/docker/volumes/18c82294a28cbc55daba3e896b330c410fc25e5da14172fb0416eec7c61307ad/_data

# 启动容器
sudo docker start clickhouse267-lwm726

# 查看所有 volume
docker volume ls
~~~

## docker启动tidb+测试cert bug
java -ea -jar target/sqlancer-2.0.0.jar   --num-threads 10 --num-queries 1000   --random-seed 0   --timeout-seconds 43200   --log-each-select true   --username root   --password 123456   --host 127.0.0.1   --port 3307   mysql   --oracle MULTI_ENGINE   --multi-engine-logical-tables 3   --multi-engine-max-rows-per-table 8

~~~
# docker启动
sudo docker run -d   --name tidb856-sqlancer-lc63   -p 15000:4000   pingcap/tidb:v8.5.6

# 验证启动成功
mysql -h 127.0.0.1 -P 15000 -uroot -e "SELECT VERSION();"

# 开新窗口
tmux new -s tidb_lc63

# sqlancer启动
cd ~/lc/sqlancer/target

java -jar sqlancer-2.0.0.jar \
  --cert-use-real-row-count true \
  --host 127.0.0.1 \
  --port 15000 \
  --username root \
  --password '' \
  --timeout-seconds 43200 \
  tidb \
  --oracle CERT
~~~

##

~~~
mysql -h127.0.0.1 -P4000 -uroot
~~~


## docker启动mysql8.0+测试cert bug

~~~
# docker启动
sudo docker run -d   --name mysql80-lc63   -p 13307:3306   -e MYSQL_ROOT_PASSWORD=123456   mysql:8.0

# 验证启动成功
mysql -h 127.0.0.1 -P 13307 -uroot -p123456 -e "SELECT VERSION();"

# 开新窗口
tmux new -s mysql_lc63

# sqlancer启动
cd ~/lc/sqlancer/target
java -jar sqlancer-2.0.0.jar \
  --cert-use-real-row-count true \
  --host 127.0.0.1 \
  --port 13307 \
  --username root \
  --password 123456 \
  --timeout-seconds 43200 \
  mysql \
  --oracle CERT
~~~

## docker启动oceanbase+测试tpl bug

~~~
# docker启动
sudo docker run -d --name my-oceanbase-lc63 -p 12881:2881 oceanbase/oceanbase-ce:latest

# 验证启动成功
mysql -h 127.0.0.1 -P 12881 -uroot@test -e "SELECT VERSION();"

# 开新窗口
tmux new -s ob_lc63

# sqlancer启动
cd ~/lc/sqlancer/target
java -jar sqlancer-2.0.0.jar \
  --host 127.0.0.1 \
  --port 12881 \
  --username root@test \
  --password '' \
  --timeout-seconds 43200 \
  oceanbase \
  --oracle TLP_WHERE
~~~

## 删掉sqlancer下cur log

~~~
find ~/lc/sqlancer/target/logs/tidb -maxdepth 1 -type f -name '*-cur.log' -print      6yrtgf4werfedrf cy6t5rty
find ~/lc/sqlancer/target/logs/tidb -maxdepth 1 -type f -name '*-cur.log' -delete
find ~/sqlancer/logs/mysql612 -maxdepth 1 -type f -name '*-cur.log' -delete
find ~/sqlancer/logs/tidb612 -maxdepth 1 -type f -name '*-cur.log' -delete
find ~/sqlancer/logs/mariadb/ -maxdepth 1 -type f -name 'vp-known*.log' -delete
find ~/sqlancer/logs/databend1/ -maxdepth 1 -type f -name '*-cur.log' -delete
~~~

## docker启动mysql9.6.0+终端直接连接

~~~
sudo docker run -d   --name mysql960-lwm68   -e MYSQL_ROOT_PASSWORD=123456   -p 13309:3306   mysql:9.6.0
mysql -h 127.0.0.1 -P 13309 -uroot -p123456 
~~~

## docker启动mysql9.7.1+终端直接连接

~~~
ALL_PROXY=socks5h://127.0.0.1:7891 sudo docker run -d --name mysql970-lwm72 -e MYSQL_ROOT_PASSWORD=123456 -p 13310:3306 mysql:9.7


mysql -h 127.0.0.1 -P 13310 -uroot -p123456 

SELECT version();
~~~


## docker启动mysql9.7.2+终端直接连接

~~~
sudo docker run -d \
  --name mysql972-lwm1013 \
  -p 127.0.0.1:13372:3306 \
  -e MYSQL_ROOT_PASSWORD='你的密码' \
  -e MYSQL_ROOT_HOST='%' \
  -v mysql972-lwm1013-data:/var/lib/mysql \
  mysql972-lwm1013:latest

mysql --protocol=TCP -h 127.0.0.1 -P 13372 -u root -p
~~~

## docker启动tidb nightly +终端直接连接
~~~
sudo docker run -d \
  --name tidb-nightly-lwm \
  -p 14005:4000 \
  -p 10085:10080 \
  pingcap/tidb:nightly

mysql -h 127.0.0.1 -P 14005 -u root
~~~


## docker启动oceanbase421+终端直接连接

~~~
sudo ss -ltnp | grep ':2881'
sudo ss -ltnp | grep ':2882'

sudo docker run -d \
  --name oceanbase421-lwm \
  -p 2881:2881 \
  -p 2882:2882 \
  --privileged \
  oceanbase421-lwm:latest

#出现 boot success!
sudo docker logs -f oceanbase421-lwm

mysql -h127.0.0.1 -P2881 -uroot

# 创建测试用户

mysql -h127.0.0.1 -P2881 -uroot@test -Doceanbase -A \
  -e "create user sqlancer identified by 'sqlancer'; grant all on *.* to sqlancer;"

mysql -h127.0.0.1 -P2881 -uroot@test -Doceanbase -A


java -jar target/sqlancer-2.0.0.jar   --num-threads 16   --num-tries 1000   --host 127.0.0.1   --port 2881   --username sqlancer@test   --password sqlancer   oceanbase   --oracle VERTICAL_PARTITIONING   --vp-query-variants-per-combination 5   --vp-helper-use-base-engine true   --vp-source-copy-schema-objects true   --vp-allow-row-deletes false   --vp-suppress-known-patterns false   --vp-enable-advanced-query-features=false   --vp-enable-deep-exploration=false
~~~


## docker启动oceanbase450+终端直接连接

~~~
sudo docker run -d \
  --name oceanbase450-lwm1013 \
  -p 127.0.0.1:12881:2881 \
  -e MODE=mini \
  -e OB_TENANT_NAME=test \
  -e OB_SYS_PASSWORD=123456 \
  -e OB_TENANT_PASSWORD=123456 \
  -v oceanbase450-lwm1013-data:/root/ob \
  -v oceanbase450-lwm1013-config:/root/.obd/cluster \
  oceanbase450-lwm1013:latest

# boot success!
sudo docker logs -f oceanbase450-lwm1013

mysql --protocol=TCP -h 127.0.0.1 -P 12881 -u 'root@test' -p123456

mysql --protocol=TCP -h 127.0.0.1 -P 12881 -u 'sqlancer@test' -psqlancer
~~~

## docker启动oceanbase712 oracle模式+终端直接连接

~~~
sudo docker run -d \
  --name oceanbase712-lwm \
  -p 2883:2881 \
  -p 2884:2882 \
  --privileged \
  oceanbase/oceanbase-ce:4.4.2.1-101000022026050611

sudo docker ps --filter name=oceanbase712-lwm
sudo docker logs -f oceanbase712-lwm

mysql -h127.0.0.1 -P2883 -uroot

mysql -h127.0.0.1 -P2883 -uroot@sys -A

CREATE RESOURCE UNIT oracle_unit
  MAX_CPU 2,
  MIN_CPU 2,
  MEMORY_SIZE '4G',
  LOG_DISK_SIZE '4G',
  MAX_IOPS 10000,
  MIN_IOPS 10000,
  IOPS_WEIGHT 1,
  MAX_SESSION_NUM 300;

CREATE RESOURCE POOL oracle_pool
  UNIT = 'oracle_unit',
  UNIT_NUM = 1,
  ZONE_LIST = ('zone1');

CREATE TENANT oracle
  CHARSET = 'utf8mb4',
  ZONE_LIST = ('zone1'),
  PRIMARY_ZONE = 'zone1',
  RESOURCE_POOL_LIST = ('oracle_pool')
  SET ob_compatibility_mode = 'oracle',
      ob_tcp_invited_nodes = '%';
~~~


## docker启动cockroach2623+终端直接连接

~~~
sudo docker run -d \
  --name cockroach2623-lwm \
  -p 26258:26257 \
  -p 8081:8080 \
  -v cockroach2623-lwm-data:/cockroach/cockroach-data \
  cockroach2623-lwm:latest \
  start-single-node --insecure \
  --listen-addr=127.0.0.1:26257 \
  --http-addr=127.0.0.1:8080

sudo docker ps -a | grep cockroach2623-lwm
sudo docker logs --tail 80 cockroach2623-lwm

psql "postgresql://root@127.0.0.1:26258/defaultdb?sslmode=disable"
~~~

## docker启动postgres18.4+终端直接连接

~~~
sudo docker pull docker.1ms.run/library/postgres:18.4-bookworm
sudo docker tag docker.1ms.run/library/postgres:18.4-bookworm postgres184-lwm:latest
sudo docker images postgres184-lwm

sudo docker run -d \
  --name postgres184-lwm \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=test \
  -p 15432:5432 \
  postgres184-lwm:latest

sudo docker logs --tail 80 postgres184-lwm

psql -h 127.0.0.1 -p 15432 -U postgres -d test

密码是： postgres

cd /home/lwm/sqlancer

java -jar target/sqlancer-2.0.0.jar \
  --num-threads 16 \
  --num-tries 1000 \
  --host 127.0.0.1 \
  --port 15432 \
  --username postgres \
  --password postgres \             
  postgres \
  --oracle VERTICAL_PARTITIONING \
  --vp-query-variants-per-combination 5 \
  --vp-helper-use-base-engine true \
  --vp-source-copy-schema-objects true \
  --vp-allow-row-deletes false \
  --vp-enable-advanced-query-features=false \
  --vp-enable-deep-exploration=false
~~~

## docker启动postgres18.6+终端直接连接

~~~
# 代理下载完成后的本地导入
sudo docker load --input /tmp/tmp.Ep8TGK20n7/postgres-18.6.tar

# 添加所需标签；未指定 :tag 时 Docker 默认为 :latest
sudo docker tag postgres:18.6 postgres186-lwm904:latest

# 校验
sudo docker image inspect \
  --format '{{.RepoTags}} {{.Id}} {{.Architecture}}/{{.Os}}' \
  postgres:18.6 postgres186-lwm904:latest

sudo docker run -d \
  --name postgres186-lwm \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=test \
  -p 15433:5432 \
  postgres186-lwm904:latest

sudo docker ps --filter name=postgres186-lwm

PGPASSWORD=postgres psql -h 127.0.0.1 -p 15433 -U postgres -d test

java -jar target/sqlancer-2.0.0.jar \
  --num-threads 16 \
  --num-tries 1000 \
  --host 127.0.0.1 \
  --port 15433 \
  --username postgres \
  --password postgres \
  postgres \
  --oracle VERTICAL_PARTITIONING \
  --vertical-partitioning-rows 10000 \
  --vertical-partitioning-max-source-rows 10000 \
  --vertical-partitioning-query-pairs 20 \
  --vp-query-variants-per-combination 5 \
  --vp-helper-use-base-engine true \
  --vp-source-copy-schema-objects true \
  --vp-allow-row-deletes false \
  --vp-suppress-known-patterns true \
  --vp-enable-advanced-query-features true \
  --vp-enable-deep-exploration true
~~~


## docker启动sqlite3.53.2+终端直接连接

~~~
mkdir -p ~/sqlite3532-data

sqlite3 ~/sqlite3532-data/test.db
SELECT sqlite_version();
.tables

.quit

cd ~/sqlancer

java -jar target/sqlancer-2.0.0.jar \
  --num-threads 16 \
  --num-tries 1000 \
  sqlite3 \
  --oracle VERTICAL_PARTITIONING \
  --vp-query-variants-per-combination 5 \
  --vp-helper-use-base-engine true \
  --vp-source-copy-schema-objects true \
  --vp-allow-row-deletes false \
  --vp-suppress-known-patterns false \
  --vp-enable-advanced-query-features=false \
  --vp-enable-deep-exploration=false
~~~


## docker启动clickhouse26.7.1+终端直接连接

~~~
sudo -E env ALL_PROXY=socks5h://127.0.0.1:7891 \
  HTTPS_PROXY=socks5h://127.0.0.1:7891 \
  HTTP_PROXY=socks5h://127.0.0.1:7891 \
  docker pull clickhouse:26.7.1.1315

sudo docker tag clickhouse:26.7.1.1315 clickhouse267-lwm726:latest

sudo docker run -d \
  --name clickhouse267-lwm726 \
  -p 18123:8123 \
  -p 19000:9000 \
  --ulimit nofile=262144:262144 \
  clickhouse267-lwm726:latest

sudo docker ps | grep clickhouse267-lwm726

sudo docker exec -it clickhouse267-lwm726 clickhouse-client

SELECT version();

CREATE USER IF NOT EXISTS sqlancer IDENTIFIED BY '123456';
GRANT CURRENT GRANTS ON *.* TO sqlancer;
SHOW GRANTS FOR sqlancer;


java -jar target/sqlancer-2.0.0.jar \
  --num-threads 8 \
  --num-queries 1000 \
  --log-each-select true \
  --host 127.0.0.1 \
  --port 8123 \
  --username sqlancer \
  --password 123456 \
  clickhouse \
  --oracle VERTICAL_PARTITIONING \
  --vp-helper-use-base-engine true \
  --vp-source-copy-schema-objects true \
  --vp-allow-row-deletes false \
  --vp-enable-update-mutations true \
  --vp-include-normal-workload true \
  --vp-suppress-known-patterns false \
  --vp-enable-advanced-query-features true \
  --vp-enable-deep-exploration true \
  --vp-enable-auto-merge-profile true \
  --vp-enable-distributed-shard-key-profile true
~~~

## docker启动clickhouse26.8.4.11+终端直接连接

~~~
sudo -E env ALL_PROXY=socks5h://127.0.0.1:7891   HTTPS_PROXY=socks5h://127.0.0.1:7891   HTTP_PROXY=socks5h://127.0.0.1:7891 \sudo docker pull clickhouse/clickhouse-server:26.8.4.11

sudo docker tag \
  clickhouse/clickhouse-server:26.8.4.11 \
  clickhouse/clickhouse-server:clickhouse268-lwm915


sudo docker run -d \
  --name clickhouse268-lwm915 \
  -p 8129:8123 \
  -p 9006:9000 \
  --ulimit nofile=262144:262144 \
  clickhouse/clickhouse-server:clickhouse268-lwm915

sudo docker ps --filter name=clickhouse268-lwm915

sudo docker exec -it clickhouse268-lwm915  clickhouse-client

SELECT version();

CREATE USER IF NOT EXISTS sqlancer IDENTIFIED BY '123456';
GRANT CURRENT GRANTS ON *.* TO sqlancer;
SHOW GRANTS FOR sqlancer;

java -jar target/sqlancer-2.0.0.jar   --num-threads 8   --num-queries 1000   --log-each-select true   --host 127.0.0.e --vp-distribut1   --port 8123   --username sqlancer   --password 123456   clickhouse   --oracle VERTICAL_PARTITIONING   --vp-helper-use-base-engine true   --vp-source-copy-schema-objects true   --vp-allow-row-deletes false   --vp-enable-update-mutations true   --vp-include-normal-workload true   --vp-suppress-known-patterns false   --vp-enable-advanced-query-features true   --vp-enable-deep-exploration true   --vp-enable-auto-merge-profile true  --vp-enable-distributed-profile true --vp-distributed-cluster cluster_3S_2R --vp-enable-distributed-shard-key-profile true
~~~



## docker启动duckdb1.5.4 +终端直接连接

~~~
把 SQLancer 的 DuckDB JDBC 依赖从 1.3.0.0 升到：
<groupId>org.duckdb</groupId>
<artifactId>duckdb_jdbc</artifactId>
<version>1.5.4.0</version>
位置：[pom.xml (line 315)](/home/lwm/sqlancer/pom.xml:315)

mvn -q -DskipTests compile执行编译自动下载

# 终端连接
conda activate duckdb154
duckdb --version
duckdb ~/sqlancer/test.duckdb

java -jar target/sqlancer-2.0.0.jar \
  --num-threads 16 \
  --num-tries 1000 \
  duckdb \
  --oracle=VERTICAL_PARTITIONING \
  --vertical-partitioning-query-pairs=20 \
  --vp-query-variants-per-combination=5 \
  --vertical-partitioning-max-source-rows=256 \
  --vp-enable-advanced-query-features=true \
  --vp-enable-deep-exploration=true \
  --vp-enable-parquet-roundtrip=true \
  --vp-enable-storage-boundary-tests=true \
  --vp-storage-boundary-profile=all \
  --vp-source-copy-schema-objects=true \
  --vp-allow-row-deletes=true \
  --vp-suppress-known-patterns=true
~~~

## docker升级duckdb到1.5.5 +终端直接连接

~~~
# 升级 DuckDB Python 包与终端 CLI
conda activate duckdb154
python -m pip install --upgrade duckdb==1.5.5
python -m pip install --upgrade duckdb-cli==1.5.5

source /home/lwm/miniconda3/etc/profile.d/conda.sh
conda activate duckdb154

duckdb --version
duckdb ~/sqlancer/test.duckdb


# --vp-split-threads=N	split 查询使用的 DuckDB 执行线程数；稳定测试建议 1
# --vp-optimizer-modes=all	轮流测试默认优化器、同时关闭两个优化器、只关 filter pushdown、只关 join order
# --vp-enable-directed-data=true	创建 DuckDB 专用的 12 列混合类型随机表
# --vp-directed-source-rows=N	定向基表生成行数，代码最少会生成 1024 行，必须配合--vertical-partitioning-max-source-rows=4096，如果写成--vertical-partitioning-max-source-rows=256虽然基表生成了 4096 行，VP source 最终只复制 256 行，无法充分覆盖 vector 边界。
# 不要同时写：--vp-source-copy-schema-objects=true --vp-schema-fidelity-mode=mixed因为前者会使 mixed 实际失效。

java -jar target/sqlancer-2.0.0.jar   --num-threads=1   --num-tries=1000   --log-each-select=true   duckdb   --oracle=VERTICAL_PARTITIONING   --vp-query-variants-per-combination=5   --vertical-partitioning-max-source-rows=4096   --vp-enable-directed-data=true   --vp-directed-source-rows=4096   --vp-enable-advanced-query-features=true   --vp-enable-deep-exploration=true   --vp-enable-parquet-roundtrip=false   --vp-enable-storage-boundary-tests=true   --vp-storage-boundary-profile=all   --vp-optimizer-modes=all   --vp-split-threads=1   --vp-schema-fidelity-mode=mixed   --vp-source-copy-schema-objects=false   --vp-allow-row-deletes=false   --vp-suppress-known-patterns=true
~~~



## docker启动CockroachDB26.2.3+sql测试

~~~
sudo docker run -d \
  --name cockroach2623-lwm \
  -p 26258:26259 \
  -p 8081:8080 \
  -v cockroach2623-lwm-data:/cockroach/cockroach-data \
  cockroach2623-lwm:latest \
  start-single-node --insecure \
  --listen-addr=127.0.0.1:26257 \
  --sql-addr=0.0.0.0:26259 \
  --http-addr=0.0.0.0:8080

sudo docker ps -a | grep cockroach2623-lwm
sudo docker logs --tail 80 cockroach2623-lwm

psql "postgresql://root@127.0.0.1:26258/defaultdb?sslmode=disable"

java -jar target/sqlancer-2.0.0.jar \
  --num-threads 16 \
  --num-tries 1000 \
  --host 127.0.0.1 \
  --port 26258 \
  --username root \
  --password "" \
  cockroachdb \
  --oracle VERTICAL_PARTITIONING \
  --vp-query-variants-per-combination 5 \
  --vp-helper-use-base-engine true \
  --vp-source-copy-schema-objects true \
  --vp-allow-row-deletes false \
  --vertical-partitioning-max-source-rows 1024 \
  --vp-suppress-known-patterns true \
  --vp-enable-advanced-query-features=true \
  --vp-enable-deep-exploration=true


~~~


## docker启动MariaDB12.3.2+sql测试

~~~
sudo -E env ALL_PROXY=socks5h://127.0.0.1:7891 \
HTTPS_PROXY=socks5h://127.0.0.1:7891 \
HTTP_PROXY=socks5h://127.0.0.1:7891 \
docker pull mariadb:12.3.2

sudo docker tag mariadb:12.3.2 mariadb12.3.2-lwm824:latest

# 验证
sudo docker images mariadb12.3.2-lwm824

#启动
sudo docker run -d \
  --name mariadb1232-lwm824 \
  -p 13306:3306 \
  -e MARIADB_ROOT_PASSWORD='123456' \
  --ulimit nofile=262144:262144 \
  mariadb12.3.2-lwm824:latest

# 终端连接
sudo docker exec -it mariadb1232-lwm824 mariadb -uroot -p123456

java -jar target/sqlancer-2.0.0.jar \
  --num-threads 16 \
  --num-tries 1000 \
  --host 127.0.0.1 \
  --port 13306 \
  --username root \
  --password 123456 \
  mariadb \
  --oracle VERTICAL_PARTITIONING \
  --vp-query-variants-per-combination 5 \
  --vp-helper-use-base-engine true \
  --vp-source-copy-schema-objects true \
  --vp-allow-row-deletes false \
  --vp-suppress-known-patterns false \
  --vp-enable-advanced-query-features=false \
  --vp-enable-deep-exploration=false

java -jar target/sqlancer-2.0.0.jar \
  --num-threads 8 \
  --num-tries 1000 \
  --host 127.0.0.1 \
  --port 13306 \
  --username root \
  --password 123456 \
  --log-each-select true \
  mariadb \
  --oracle VERTICAL_PARTITIONING \
  --vp-query-variants-per-combination 5 \
  --vp-helper-use-base-engine true \
  --vp-source-copy-schema-objects true \
  --vp-allow-row-deletes false \
  --vp-suppress-known-patterns true \
  --vp-enable-advanced-query-features true \
  --vp-enable-deep-exploration true
~~~

Operating system:
- Ubuntu 20.04.6 LTS (Focal Fossa)
- Linux kernel: 5.4.0-204-generic
- Architecture: x86_64

Hardware:
- CPU: 2 × Intel Xeon Gold 5218R @ 2.10 GHz
- 40 physical cores / 80 logical CPUs


##  docker启动MariaDB 13.0.2+sql测试

~~~
sudo docker run -d \
  --name mariadb13.0.2-lwm1013 \
  -p 127.0.0.1:13382:3306 \
  -e MARIADB_ROOT_PASSWORD=123456 \
  -e MARIADB_ROOT_HOST='%' \
  -v mariadb13.0.2-lwm1013-data:/var/lib/mysql \
  mariadb13.0.2-lwm1013:latest

##ready for connections
sudo docker logs -f mariadb13.0.2-lwm1013

mysql --protocol=TCP -h 127.0.0.1 -P 13382 -u root -p123456
~~~


## docker启动databend:v1.2.925-patch-11 +终端直接连接

~~~
sudo docker pull datafuselabs/databend:v1.2.925-patch-11

sudo docker tag datafuselabs/databend:v1.2.925-patch-11 databend1.2.925-lwm826

sudo docker run -d \
  --name databend1.2.925-lwm826 \
  -p 3307:3307 \
  -p 8000:8000 \
  -p 18124:8124 \
  -p 8900:8900 \
  --restart unless-stopped \
  databend1.2.925-lwm826

sudo docker exec -it databend1.2.925-lwm826 /bin/bash

#如果容器内没有 mysql 客户端，从宿主机执行即可
mysql --protocol=tcp -h127.0.0.1 -P3307 -uroot

# 容器内执行：
mysql -h 127.0.0.1 -P 3307 -u root


java -jar target/sqlancer-2.0.0.jar \
  --host 127.0.0.1 \
  --port 3307 \
  --username root \
  --password '' \
  --num-threads 8 \
  --num-tries 1000 \
  --log-each-select true \
  databend \
  --oracle VERTICAL_PARTITIONING \
  --vp-query-variants-per-combination 5 \
  --vp-helper-use-base-engine true \
  --vp-source-copy-schema-objects true \
  --vp-allow-row-deletes false \
  --vp-suppress-known-patterns true \
  --vp-enable-deep-exploration true \
  --vp-max-source-rows 4096 \
  --vp-mixed-helper-engines true

java -jar target/sqlancer-2.0.0.jar   --host 127.0.0.1   --port 3307   --username root   --password ''   --num-threads 8   --num-tries 1000  databend   --oracle VERTICAL_PARTITIONING   --vp-query-variants-per-combination 5   --vp-helper-use-base-engine false   --vp-source-copy-schema-objects true   --vp-allow-row-deletes false   --vp-suppress-known-patterns true   --vp-enable-deep-exploration true   --vp-max-source-rows 4096 --vp-mixed-helper-engines false --vp-engine-layout-profile FUSE_FOCUSED

java -jar target/sqlancer-2.0.0.jar   --host 127.0.0.1   --port 3307   --username root   --password ''   --num-threads 8   --num-tries 1000  databend   --oracle VERTICAL_PARTITIONING   --vp-query-variants-per-combination 5   --vp-helper-use-base-engine false   --vp-source-copy-schema-objects true   --vp-allow-row-deletes false   --vp-suppress-known-patterns true   --vp-enable-deep-exploration true   --vp-max-source-rows 4096 --vp-mixed-helper-engines false --vp-engine-layout-profile FUSE_FOCUSED
~~~

## ## docker启动databend:v1.2.950-lwm1015 +终端直接连接

~~~
sudo docker run -d \
  --name databend1.2.950-lwm1015 \
  -p 13307:3307 \
  -p 18000:8000 \
  -p 18125:8124 \
  -p 18900:8900 \
  --restart unless-stopped \
  databend1.2.950-lwm1015

#查看启动状态：
sudo docker ps --filter name=databend1.2.950-lwm1015

#进入容器终端：
sudo docker exec -it databend1.2.950-lwm1015 /bin/bash

#从宿主机连接 MySQL：
mysql --protocol=tcp \
  -h127.0.0.1 \
  -P13307 \
  -uroot

~~~


## docker启动MonetDB 11.55.7 +终端直接连接

~~~
# 1. 拉取官方镜像
sudo docker pull monetdb/monetdb:Dec2025-SP3

# 2. 修改本地 tag
sudo docker tag \
  monetdb/monetdb:Dec2025-SP3 \
  monetdb/monetdb:MonetDB11.55.7-lwm914

# 创建持久化 volume
sudo docker volume create monetdb11557-data

# 启动容器
sudo docker run -d \
  --name monetdb11557-lwm914 \
  --restart unless-stopped \
  -e MDB_DB_ADMIN_PASS=monetdb \
  -p 50000:50000 \
  -v monetdb11557-data:/var/monetdb \
  monetdb/monetdb:MonetDB11.55.7-lwm914

检查状态和日志：
sudo docker ps --filter name=monetdb11557-lwm914

sudo docker logs -f monetdb11557-lwm914

连接测试：
sudo docker exec -it monetdb11557-lwm914 \
  mclient -d monetdb -u monetdb
出现 password: 后输入：
monetdb

也可以从宿主机连接：
mclient -h 127.0.0.1 -p 50000 -d monetdb -u monetdb

java -jar target/sqlancer-2.0.0.jar   --num-threads 8   --num-tries 1000   --log-each-select true   --host 127.0.0.1   --port 50000   --username monetdb   --password monetdb   monetdb   --oracle VERTICAL_PARTITIONING   --vp-query-variants-per-combination 5   --vertical-partitioning-max-source-rows 4096  --vp-helper-use-base-engine true   --vp-source-copy-schema-objects true   --vp-allow-row-deletes false   --vp-suppress-known-patterns false   --vp-enable-advanced-query-features false   --vp-enable-deep-exploration true

~~~

## docker启动cockroachdb26.2.3三节点

~~~
# 启动
sudo -n docker start crdb1 crdb2 crdb3

# 运行 SQLancer 前先确认三个端口对应不同节点：
for port in 26261 26259 26260; do
  psql "postgresql://root@127.0.0.1:${port}/defaultdb?sslmode=disable" \
    -Atc "SELECT '${port}', crdb_internal.node_id();"
done

# 再确认集群状态：
psql 'postgresql://root@127.0.0.1:26261/defaultdb?sslmode=disable' \
  -c "SELECT node_id, address, sql_address, is_live
      FROM crdb_internal.kv_node_status
      ORDER BY node_id;"


#完整的三节点分布式测试必须组合：
  --vp-enable-distributed-profile true
  --vp-distributed-gateways node1:port,node2:port,node3:port
  --vp-distributed-min-live-nodes 3
  三个 gateway 必须连接同一集群，并且最好分别直连三个不同节点。
#完整 row/range 压力测试应组合：
  --vp-enable-distributed-profile true
  --vp-enable-row-range-pressure-profile true
  --vp-row-range-pressure-max-source-rows 4096
  --vertical-partitioning-max-source-rows 4096
  代码允许单独打开 row-range profile，但没有 distributed profile 时无法完整覆盖 gateway 切换、range placement、scatter 和 DistSQL 差异。
#Time travel 必须配合 VP：
--oracle VERTICAL_PARTITIONING
--vp-enable-time-travel-profile true
它可以和 distributed、row-range profile 同时使用，但会明显增加执行时间和 schema 压力。若集群仍有大量 schema job backlog，第一轮长跑可先设为 false。


java -ea -jar target/sqlancer-2.0.0.jar   --host 127.0.0.1   --port 26261   --username root   --password ''   --num-threads 2   --num-tries 1000   --log-each-select true   cockroachdb   --oracle VERTICAL_PARTITIONING   --vp-query-variants-per-combination 5   --vertical-partitioning-max-source-rows 4096   --vp-source-copy-schema-objects true   --vp-helper-use-base-engine true   --vp-allow-row-deletes false   --vp-enable-advanced-query-features true   --vp-enable-deep-exploration true   --vp-enable-time-travel-profile false   --vp-enable-distributed-profile true   --vp-enable-row-range-pressure-profile true   --vp-row-range-pressure-max-source-rows 4096   --vp-distributed-gateways 127.0.0.1:26261,127.0.0.1:26259,127.0.0.1:26260   --vp-distributed-min-live-nodes 3   --vp-suppress-known-patterns true
~~~


## docker启动clickhouse26.8.4.11三节点

~~~
cd /home/lwm/sqlancer/clickhouse-268-cluster
#拉取镜像：
sudo -E env \
  ALL_PROXY=socks5h://127.0.0.1:7891 \
  HTTPS_PROXY=socks5h://127.0.0.1:7891 \
  HTTP_PROXY=socks5h://127.0.0.1:7891 \
  docker pull clickhouse/clickhouse-server:26.8.4.11

sudo -E env \
  ALL_PROXY=socks5h://127.0.0.1:7891 \
  HTTPS_PROXY=socks5h://127.0.0.1:7891 \
  HTTP_PROXY=socks5h://127.0.0.1:7891 \
  docker pull clickhouse/clickhouse-keeper:26.8.4.11
#检查配置：
sudo docker-compose config
#启动新集群：
sudo docker-compose up -d
#查看容器状态：
sudo docker-compose ps
#新集群使用固定网段：172.30.250.0/28
#端口：
ClickHouse HTTP:   8130-8135
ClickHouse Native: 9007-9012
Keeper:            9191-9193
#验证版本：
sudo docker exec ch268-clickhouse1 \
  clickhouse-client \
  --user sqlancer \
  --password 123456 \
  --query "SELECT version()"
#验证 cluster_3S_2R：
sudo docker exec ch268-clickhouse1 \
  clickhouse-client \
  --user sqlancer \
  --password 123456 \
  --query "
SELECT
    cluster,
    shard_num,
    replica_num,
    host_name,
    host_address,
    port
FROM system.clusters
WHERE cluster = 'cluster_3S_2R'
ORDER BY shard_num, replica_num
FORMAT PrettyCompact
"
#验证六个节点都可达：
sudo docker exec ch268-clickhouse1 \
  clickhouse-client \
  --user sqlancer \
  --password 123456 \
  --query "
SELECT countDistinct(hostName())
FROM clusterAllReplicas('cluster_3S_2R', system.one)
"
预期结果：
6
#验证 HTTP 入口：
curl -fsS \
  'http://127.0.0.1:8130/?user=sqlancer&password=123456' \
  --data-binary 'SELECT currentUser(), version() FORMAT TSV'
预期类似：
sqlancer    26.8.4.11

java -jar target/sqlancer-2.0.0.jar \
  --num-threads 8 \
  --num-queries 1000 \
  --log-each-select true \
  --host 127.0.0.1 \
  --port 8130 \
  --username sqlancer \
  --password 123456 \
  clickhouse \
  --oracle VERTICAL_PARTITIONING \
  --vp-helper-use-base-engine true \
  --vp-source-copy-schema-objects true \
  --vp-allow-row-deletes false \
  --vp-enable-update-mutations true \
  --vp-include-normal-workload true \
  --vp-suppress-known-patterns false \
  --vp-enable-advanced-query-features true \
  --vp-enable-deep-exploration true \
  --vp-enable-auto-merge-profile true \
  --vp-enable-distributed-profile true \
  --vp-distributed-cluster cluster_3S_2R \
  --vp-enable-distributed-shard-key-profile true

~~~




## 终端直接连接数据库

~~~
# tidb856
mysql -h 127.0.0.1 -P 14000 -u root

# mysql9.6.0
mysql -h 127.0.0.1 -P 13309 -uroot -p123456 

# mysql8.0.46
mysql -h 127.0.0.1 -P 13307 -uroot -p123456
~~~

## VP实验

~~~
mvn -q -DskipTests compile
mvn -q -DskipTests package

java -jar target/sqlancer-*.jar   --host 127.0.0.1   --port 13309   --username root   --password 123456   --num-threads 16 --num-tries 1000  mysql   --oracle VERTICAL_PARTITIONING   --vertical-partitioning-query-pairs 20   --vertical-partitioning-max-source-rows 128

# 
java -jar target/sqlancer-2.0.0.jar   --host 127.0.0.1   --port 14000   --username root   --password ""   tidb   --num-threads 16 --num-tries 1000 --oracle VERTICAL_PARTITIONING   --vertical-partitioning-query-pairs 20   --vertical-partitioning-max-source-rows 256

# MySQL 9.6.0：
java -jar target/sqlancer-2.0.0.jar mysql \
  --oracle VERTICAL_PARTITIONING \
  --num-threads 16 \
  --num-tries 1000 \
  --host 127.0.0.1 \
  --port 13309 \
  --username root \
  --password 123456 \
  --vp-query-variants-per-combination 5

# TiDB nightly：
java -jar target/sqlancer-2.0.0.jar \
  --num-threads 16 \
  --num-tries 1000 \
  --host 127.0.0.1 \
  --port 14005 \
  --username root \
  --password '' \
  tidb \
  --oracle VERTICAL_PARTITIONING \
  --vp-query-variants-per-combination 5
~~~






## 最新版 tidb build + 开启+连接

~~~
cd /home/lwm/tidb

# 开代理因为会拉取 github
env GOPATH=/home/lwm/go \
PATH=/usr/local/go/bin:/usr/bin:/bin:/home/lwm/go/bin \
CGO_ENABLED=1 \
GO111MODULE=on \
go build \
  -ldflags '-X "github.com/pingcap/tidb/pkg/parser/mysql.TiDBReleaseVersion=v9.0.0-latest-lwm" -X "github.com/pingcap/tidb/pkg/util/versioninfo.TiDBGitHash=b1d4142f99" -X "github.com/pingcap/tidb/pkg/util/versioninfo.TiDBGitBranch=origin/master" -X "github.com/pingcap/tidb/pkg/util/versioninfo.TiDBEdition=Community"' \
  -o bin/tidb-server-latest-lwm \
  ./cmd/tidb-server

# 编译完成后检查：
ls -lh /home/lwm/tidb/bin/tidb-server-latest-lwm

/home/lwm/tidb/bin/tidb-server-latest-lwm -V

# 启动新实例
rm -rf /home/lwm/tidb-data/latest-lwm
mkdir -p /home/lwm/tidb-data/latest-lwm

cd /home/lwm/tidb

./bin/tidb-server-latest-lwm \
  --store=unistore \
  --path=/home/lwm/tidb-data/latest-lwm \
  -P 14006 \
  -status 11086

# 连接
mysql -h 127.0.0.1 -P 14006 -uroot 
~~~


## tiup开启tidb 集群 + 运行sqlancer

~~~
tiup playground nightly \
  --db 1 \
  --pd 1 \
  --kv 1 \
  --host 127.0.0.1 \
  --db.port 14007 \
  --pd.port 23799 \
  --kv.port 20170 \
  --without-monitor \
  --tag sqlancer-nightly


# 验证版本 9.0

mysql -h 127.0.0.1 -P 14007 -uroot -e 'SELECT tidb_version();'

# 连接

mysql -h 127.0.0.1 -P 14007 -uroot

java -jar target/sqlancer-2.0.0.jar   --num-threads 16   --num-tries 1000   --host 127.0.0.1   --port 14007   --username root   --password ''   tidb   --oracle VERTICAL_PARTITIONING   --vp-query-variants-per-combination 50 --vp-helper-use-base-engine true --vp-source-copy-schema-objects true
~~~



单列表：约 20%
多列表：约 80%

EMPTY：固定使用 EMPTY_SPLIT
SINGLE_COLUMN：在 3 个单列表策略中均匀随机选
MULTI_COLUMN：在 4 个多列表策略中均匀随机选

选 base 表
  -> 判断 EMPTY / SINGLE_COLUMN / MULTI_COLUMN
  -> 选择 split strategy
  -> 物化 source 表
  -> 物化 left/right 垂直分区表
  -> 针对同一组 left/right 分区，调度多种 reconstruction strategy
  -> 生成 singleQuery 和 splitQuery
  -> 执行并比较两个结果 multiset
  -> 没 bug 就清理 vp_* 临时对象

调度所有 reconstruction enum

每次 oracle run 只选一张 base 表，只产生一套 split schema，但会在这套 split schema 上跑多种 reconstruction 策略。

每个 reconstruction pair 怎么生成
对每个 reconstruction strategy流程是：
createSourceAst(vp)
  -> 用 vp_source 的列构造 AST 里的逻辑表

生成一个随机 SELECT，FROM 是 vp_source
  -> select list 随机表达式/聚合
  -> 可选 WHERE
  -> 可选 GROUP BY / HAVING
  -> 可选 ORDER BY
  -> 可选 LIMIT / OFFSET

singleQuery = SELECT ... FROM vp_source ...

relation = buildReconstructionRelation(vp, strategy, extraSetup)

splitQuery = 把 singleQuery 里的 FROM vp_source
             替换成 FROM <relation>
也就是说，它不是分别随机生成两个查询，而是：
singleQuery: 对 source 表查询
splitQuery: 同一个查询，但 FROM source 替换成“由 left/right 重构出来的 relation”
这保证了比较对象是“原始 source 视图”和“分区重构视图”的同构查询。

mvn -q -DskipTests compile
mvn -q -DskipTests package

--oracle VERTICAL_PARTITIONING   --vp-query-variants-per-combination 5 --vp-helper-use-base-engine true --vp-source-copy-schema-objects true --vp-allow-row-deletes false --vp-suppress-known-patterns true --vp-enable-advanced-query-features=true现在无论是mysql还是tidb的运行参数基本都是这些，只不过--vp-enable-advanced-query-features=true可能会换成--vp-enable-deep-exploration=true，我希望你熟悉我们这个oracle的实现层级和细节，因为计划在cockroachdb上也实现，你觉着需要注意哪些地方？以及你是否有要跟我确认的地方

Known suppress我们实现这个接口但是池为空，因为我们还没有已知bug。VP oracle迁移 advanced/deep 全功能，charset/collation 支持可能和 MySQL/TiDB 不同，deep 模式第一批最好保守。无论是哪块与现有不同，你应该是去适配，而不是阉割我们的功能，那样会漏掉bug。虽然我会先测试base模式，但是既然我们决定迁移就一步到位，这样跑完基础模式可以立马测试增强模式，

既然我们之前的改动都没有落盘。那么直接重新执行这个迁移任务。迁移目标是 功能等价，不是代码逐字等价。在OceanBase实现MySQL 模式。根据我们前面的所有沟通内容迁移实现

注意FLOAT/DOUBLE approximate tolerance不要给太大，以免错过bug。MySQL/TiDB 的这些能力并不是散点，而是围绕 BaseShape -> compatible split schedule -> materialize per split -> all reconstructions -> AST outer query -> tolerant comparison -> rich stats/report 这条链路组织的。基础模式，两个增强模式以及其它的功能、流程与另两个数据库一致实现，不要做成近似实现，是严格落地。新增超大文件时经常出现代码保存中断，本次实现你用最稳妥的方式迁移，即使中途因故退出，下次可以接着补，而不是全都回退到空

因为前面有很长时间的网络中断，所以需要确保我们在OceanBase上实现的VP和另两个数据库是一致的。不要凭你刚刚实现的感觉，而是仔细审查代码的实现，确认是否实现了和mysql/tidb一样的功能而没有阉割，不同层级不同参数是否都实现到位？以及告诉我有哪些功能是OceanBase上不支持而你为了对其功能手动实现的


看一下logs/oceanbase630/的日志，为什么15个h一个bug都没测出来，这很反常，你具体看看日志反应的我们期望运行的各环节是否正常，是不是我们迁移代码的时候有地方有问题

我要求你重新审视代码中OceanBase和MySQL/TiDB的实现，各环节逐一对比向我汇报，审查还有哪些实现不一致的地方。你要切实去看两边的代码，而不是凭记忆凭感觉

这次的15h oceanbase仍然没有测出有效bug，你看一下sqlancer/oceanbase/和sqlancer/mysql/和sqlancer/tidb/的日志，因为非常多不用全看，挑几个就行，看看能不能找到mei有bug的原因或者发现我们迁移代码的不完善处


查看sqlancer/logs/oceanbase712/下各复现的bug.log，我都写了根因，除了16bug.log是输出顺序的假阳性，其它bug是否是之前的重复bug？你可以查阅oceanbase72、73、78/下其它过往bug记录，也可以是查看known池，；另外你审查一下我们known池中加入的是不是界限过宽了，看看sqlancer/logs/oceanbase/vp-known.log是不是有错滤的bug

查看sqlancer/logs/oceanbase712/下复现的bug.log和database.log是否对应，没有出现不和原日志一样的bug吧？

Claude：查看sqlancer cockroachdb数据库的 VERTICAL_PARTITIONING oracle实现，是否有功能缺陷？现在一个bug也没测到，以及针对这个数据库特有的功能特性可以做哪些探索

Codex：postgres VERTICAL_PARTITIONING oracle 始终没有测到有效bug，你审查一下代码，看看是否有背离VP oracle 的地方，是否有自身代码缺陷，是否有矫枉过正的地方等等，再结合这个数据库本身的特性思考一下可以再在哪些地方探索

CodexZ：但是我们在sqlite3上运行了很久都没测到有效bug，基于我们这个VP idea的思想，你觉着我们现在实现上是否有缺陷？我们可以针对sqlite3特有的数据库模式做哪些改进？

Codex再测：vp oracle 在mariadb上的实现，除了之前测出的几个bug，此后再也测不出bug来，你看一下sqlancer/logs/mariadb/下的日志，审查一下实现代码，看看是否有实现缺陷，是否有过度实现或者偏离vp oracle的地方，该如何继续改进从而让mariadb的bug暴露，被vp oracle测到

SQLANCER_DQR_METRICS=true \
java -Ddqr.maxComparisonRows=10000 \
  -jar /home/lwm/DQR/target/sqlancer-2.0.0.jar \
  --random-seed 0 \
  --num-threads 8 \
  --num-tries 1000 \
  --database-prefix duckdbdqr \
  duckdb \
  --oracle TIME_EQUIV \
  --dqr-norec-attribution true

find ~/DQR/logs/duckdb1/ -maxdepth 1 -type f -name '*-cur.log' -delete

python3 /home/lwm/extract_session_chinese.py ""
