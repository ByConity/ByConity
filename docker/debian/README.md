## About
Debian image with byconity dependencies and common operational tools.
The foundationdb version is 7.1.27 (without avx supports).
If you need avx, changes the foudationdb to 7.1.26 (or any version from release page) and rebuild the image.
To build the image, run:

```bash
docker build .
```