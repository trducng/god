DISK="abcxyz/sda/"
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches
sudo blockdev --flushbufs $DISK
sudo hdparm -F $DISK
