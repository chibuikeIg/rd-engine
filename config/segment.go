package config

const MFS int64 = 500 // Maximum file/segment size
const SegmentStorageBasePath = "./storage/segments"
const HintFileStoragePath = "./storage/hint-files"
const Manifest = "manifest.txt"
const HashTableSize = 50
const WriteRequestBufferSize = 150
const ToDeleteSegmentBufferSize = 200
const KeyDirPersistThreshold = 3
