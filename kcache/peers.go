package kcache

// peers 模块

// Picker 定义了获取分布式节点的能力
type Picker interface {
	Pick(key string) (Peer, bool)
}

// Peer 定义了从远端获取缓存的能力
// 所以每个Peer应实现这个接口
type Peer interface {
	GetFromPeer(group string, key string) ([]byte, int, error)
	SetFromPeer(group string, key, val string, nx int) error
	DelFromPeer(group string, key string) error
}
