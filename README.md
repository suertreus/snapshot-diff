`snapshot-diff` is an offline, userspace counterpart to Linux's device-mapper snapshot target.

Rather than intercepting writes as they occur and shunting them to a separate difference device, this tool compares an origin and a destination device at rest and produces a difference file such that a snapshot device based on the origin device and the difference file is identical to the destination file.

The tool supports reading and writing sparse files efficiently (although it can also read from block devices and even pipes).  When used with a sparse device image such as can be produced by `ntfsclone` (_not_ its "special image format"), a clean image and a tree of incremental modifications can be stored very compactly, and any snapshot can be mounted using `dmsetup`.

Typical use with `dmsetup` looks like:
```bash
losetup --read-only --find "${origin}"
losetup --read-only --find "${difference}"
dmsetup create "${name}" --readonly --table "0 $((${stat --format=%s "${origin}") / 512)) snapshot $(losetup --associated "${origin}" --output NAME --noheadings) $(losetup --associated "${difference}" --output NAME --noheadings) P 2048"

cmp "/dev/mapper/${name}" "${destination}"  # should be the same

dmsetup remove --retry "${name}"
losetup --detach "$(losetup --associated "${origin}" --output NAME --noheadings)" "$(losetup --associated "${difference}" --output NAME --noheadings)"
```
