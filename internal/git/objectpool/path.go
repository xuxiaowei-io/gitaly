package objectpool

import (
	"path/filepath"
)

// FullPath on disk, depending on the storage path, and the pools relative path
func (o *ObjectPool) FullPath() string {
	return filepath.Join(o.storagePath, o.GetRelativePath())
}
