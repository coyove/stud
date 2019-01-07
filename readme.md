# Stud: Simple Tree Upon Disk
Stud is a KV storage, a simple BTree which can be persistently stored on disk. It was designed to store millions of files as a whole.

To make it simple enough, there is no deletion support - you can only add new keys. There is no transaction support either, because it doesn't help much if you tend to write values much larger than keys. 
