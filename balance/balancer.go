package balance

type Balancer interface {
	// Add 添加子项
	Add(params ...Addr) error
	// Delete 删除子项
	Delete(params string)
	// Next 获取下一个值
	Next() string
}
