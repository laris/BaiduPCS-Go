#export GOPATH=$HOME/.go
#export PATH=$PATH:~/.go/bin
#mkdir -p ~/.go/bin

go build -v -x
echo cp -f BaiduPCS-Go ~/.go/bin/
cp -f BaiduPCS-Go ~/.go/bin/
ln -sf ~/.go/bin/BaiduPCS-Go ~/.go/bin/bpcs
ls -l ~/.go/bin
date
