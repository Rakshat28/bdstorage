#!/bin/bash
mkdir -p /tmp/bench_data
cd /tmp/bench_data

echo "Setting up Arena 1: The Sparse Hash Flex (Large files, tiny differences)..."
mkdir -p arena_sparse/pristine
dd if=/dev/urandom of=arena_sparse/pristine/master.bin bs=1M count=100
cp arena_sparse/pristine/master.bin arena_sparse/pristine/dup1.bin
cp arena_sparse/pristine/master.bin arena_sparse/pristine/modified.bin
# Overwrite 1 byte in the middle to trigger the sparse-hash rejection
printf '\xFF' | dd of=arena_sparse/pristine/modified.bin bs=1 seek=50000000 count=1 conv=notrunc

echo "Setting up Arena 2: Massive exact duplicates (Reflink speed flex)..."
mkdir -p arena_reflink/pristine
dd if=/dev/urandom of=arena_reflink/pristine/master.bin bs=1M count=50
for i in {1..20}; do cp arena_reflink/pristine/master.bin arena_reflink/pristine/dup$i.bin; done

echo "Setting up Arena 3: The Deep Tree (Thousands of tiny files)..."
mkdir -p arena_tiny/pristine
for i in {1..100}; do
  mkdir -p arena_tiny/pristine/dir_$i
  for j in {1..50}; do
    # Create 15,000 total files (10,000 duplicates, 5,000 unique)
    echo "This is identical duplicate A" > arena_tiny/pristine/dir_$i/file_A_$j.txt
    echo "This is identical duplicate B" > arena_tiny/pristine/dir_$i/file_B_$j.txt
    echo "This is unique file $i $j" > arena_tiny/pristine/dir_$i/unique_$j.txt
  done
done