package skiplist

type Node struct {
	key  interface{}
	next []*Node
}

func newNode(key interface{}, height int) *Node {
	x := &Node{
		key:  key,
		next: make([]*Node, height),
	}

	return x
}

func (node *Node) getNext(level int) *Node {
	return node.next[level]
}

func (node *Node) setNext(level int, x *Node) {
	node.next[level] = x
}
