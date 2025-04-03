package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
	"time" // 用于日志或调试时间戳
)

// ==========================================================================
// 常量定义
// ==========================================================================

const (
	// DefaultPageSize 定义了数据库页面的默认大小（例如 4KB）。
	// 页面大小影响 B+树的度和性能，通常是操作系统页面大小的倍数。
	DefaultPageSize = 4096

	// magicNumber 是一个固定的幻数，用于标识文件类型和基本完整性检查。
	magicNumber = uint32(0xBEEFCAFE) // B+树文件的幻数

	// metaPageID 定义了存储元数据（如根节点ID）的页面ID，通常是第一个页面。
	metaPageID = PageID(0)

	// defaultMinKeysPerNode 定义了 B+树节点的最小键数（t-1）。
	// B+树的“度”(t) 决定了节点的容量。一个节点最多有 2t-1 个键，最少有 t-1 个键（根节点除外）。
	// 这里设置 minKeys = 2，意味着 t = 3。 MaxKeys = 2*3 - 1 = 5。
	defaultMinKeysPerNode = 2

	// nodeHeaderBaseSize 是节点头部固定部分的大小（不包括校验和和可变部分如 nextLeaf）。
	// isLeaf(1) + numKeys(2) + padding(1) = 4 字节
	nodeHeaderBaseSize = 4

	// pagePointerSize 是存储页面ID的大小（通常是 uint64）。
	pagePointerSize = 8 // 存储 PageID (uint64) 的大小

	// nodePointerAreaSize 是内部节点中存储子节点指针偏移量等信息的区域大小。
	// 存储 keysOffset(2) + childrenOffset(2) 的大小 = 4 字节
	internalNodePointerAreaSize = 4

	// leafNodePointerAreaSize 是叶子节点中存储指针偏移量等信息的区域大小。
	// 存储 keysOffset(2) + valuesOffset(2) 的大小 = 4 字节
	leafNodePointerAreaSize = 4

	// checksumSize 是存储校验和的空间大小。
	checksumSize = 4 // CRC32 校验和占用 4 字节
)

// ==========================================================================
// 错误定义
// ==========================================================================

var (
	ErrKeyNotFound        = errors.New("键未找到")
	ErrKeyExists          = errors.New("键已存在")
	ErrNodeFull           = errors.New("节点已满（应在内部处理）")
	ErrChecksumMismatch   = errors.New("页面校验和不匹配，可能已损坏")
	ErrInvalidPageID      = errors.New("无效的页面ID")
	ErrInvalidMagicNumber = errors.New("元数据中的幻数无效")
	ErrInvalidPageSize    = errors.New("数据库文件页面大小与配置不符")
	ErrDataTooLarge       = errors.New("数据大小超过页面限制")
	ErrNodeSplitFailed    = errors.New("节点分裂失败")
	ErrNodeWriteFailed    = errors.New("节点写入失败")
	ErrNodeReadFailed     = errors.New("节点读取失败")
	ErrMetaWriteFailed    = errors.New("元数据写入失败")
	ErrMetaReadFailed     = errors.New("元数据读取失败")
	ErrPagerClosed        = errors.New("页面管理器已关闭")
)

// ==========================================================================
// Page 和 Pager (页面管理器)
// ==========================================================================

// PageID 代表磁盘上页面的唯一标识符。
type PageID uint64

// Page 代表从磁盘读取或写入的一个固定大小的数据块。
type Page []byte

// Pager 负责管理页面与磁盘文件之间的读写操作，并提供缓存。
type Pager struct {
	file         *os.File        // 底层数据库文件句柄
	pageSize     int             // 页面大小 (字节)
	numPages     PageID          // 文件中当前的总页面数
	fileSize     int64           // 文件当前大小 (字节)
	mu           sync.RWMutex    // 保护 Pager 内部状态（numPages, fileSize, cache, dirty）的读写锁
	pageCache    map[PageID]Page // 简单的页面缓存 (未来可替换为 LRU 等策略)
	dirtyPages   map[PageID]bool // 跟踪缓存中被修改的页面（脏页）
	maxCacheSize int             // 缓存中允许的最大页面数 (简单的容量控制)
	closed       bool            // 标记 Pager 是否已关闭
}

// NewPager 创建或打开一个数据库文件，并初始化 Pager。
// filename: 数据库文件名。
// pageSize: 页面大小。
// maxCacheSize: 缓存中的最大页面数量。
func NewPager(filename string, pageSize int, maxCacheSize int) (*Pager, error) {
	if pageSize <= 0 || pageSize%(checksumSize*2) != 0 { // 页面大小需合理且为校验和大小的倍数
		return nil, fmt.Errorf("无效的页面大小 %d：必须为正数且通常是 %d 的倍数", pageSize, checksumSize*2)
	}

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666) // 读写模式打开，不存在则创建
	if err != nil {
		return nil, fmt.Errorf("打开数据库文件 '%s' 失败: %w", filename, err)
	}

	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("获取文件 '%s' 信息失败: %w", filename, err)
	}

	fileSize := fi.Size()
	numPages := PageID(0)
	expectedPageSize := pageSize // 期望的页面大小

	if fileSize > 0 {
		// 如果文件已存在且非空
		if fileSize < int64(pageSize) { // 文件大小至少应够一个元数据页
			file.Close()
			return nil, fmt.Errorf("数据库文件 '%s' 大小 (%d) 小于指定的页面大小 (%d)", filename, fileSize, pageSize)
		}

		// 尝试读取元数据页来确定实际的页面大小（如果文件是之前创建的）
		tempPageData := make([]byte, pageSize)
		n, readErr := file.ReadAt(tempPageData, int64(metaPageID)*int64(pageSize)) // 尝试读取第一个页（元数据页）

		// 文件大小不是当前页面大小的整数倍，可能是损坏或页面大小不匹配
		if fileSize%int64(pageSize) != 0 || (readErr != nil && !errors.Is(readErr, io.EOF)) || (readErr == nil && n != pageSize) {
			// 文件可能使用了不同的页面大小，尝试从元数据中恢复
			// 注意：这里的恢复逻辑比较基础，假设元数据本身没损坏
			fmt.Printf("警告：文件大小 %d 不是页面大小 %d 的倍数，或读取元数据页失败。尝试从元数据中确定页面大小。\n", fileSize, pageSize)

			// 尝试读取可能的元数据头来获取存储的页面大小
			// （这假设了元数据结构，需要与 MetaData 结构匹配）
			if fileSize >= int64(checksumSize+4+8+4) { // 至少包含 checksum + magic + rootID + pageSize
				headerData := make([]byte, checksumSize+4+8+4)
				_, headerErr := file.ReadAt(headerData, 0) // 读取文件开头
				if headerErr == nil {
					// 校验 checksum (如果需要，但启动时可能跳过严格校验)
					// 读取存储的 magic number
					storedMagic := binary.LittleEndian.Uint32(headerData[checksumSize : checksumSize+4])
					if storedMagic == magicNumber {
						// 读取存储的页面大小
						storedPageSize := int(binary.LittleEndian.Uint32(headerData[checksumSize+4+8 : checksumSize+4+8+4]))
						if storedPageSize > 0 && storedPageSize != pageSize {
							fmt.Printf("警告：文件 '%s' 使用的页面大小为 %d，与配置的 %d 不同。将使用文件中的页面大小。\n", filename, storedPageSize, pageSize)
							expectedPageSize = storedPageSize // 使用文件中记录的页面大小
						} else if storedPageSize == pageSize {
							// 文件中的页面大小与配置一致，但文件大小仍有问题
							file.Close()
							return nil, fmt.Errorf("数据库文件 '%s' 大小 %d 对于页面大小 %d 无效，可能已损坏", filename, fileSize, pageSize)
						}
					} else {
						// 幻数不匹配，文件格式不对或严重损坏
						file.Close()
						return nil, fmt.Errorf("数据库文件 '%s' 的幻数不匹配，文件无效或已损坏", filename)
					}
				} else {
					// 读取头部失败
					file.Close()
					return nil, fmt.Errorf("读取数据库文件 '%s' 头部失败以确定页面大小: %w", filename, headerErr)
				}
			} else {
				// 文件太小，无法包含有效的元数据头
				file.Close()
				return nil, fmt.Errorf("数据库文件 '%s' 过小 (%d bytes)，无法包含有效的元数据", filename, fileSize)
			}
			// 如果页面大小被修正，重新检查文件大小
			if fileSize%int64(expectedPageSize) != 0 {
				file.Close()
				return nil, fmt.Errorf("数据库文件 '%s' 大小 %d 不是检测到的页面大小 %d 的整数倍，文件可能已损坏", filename, fileSize, expectedPageSize)
			}
		}
		// 根据最终确定的页面大小计算页面数量
		numPages = PageID(fileSize / int64(expectedPageSize))
	}

	// 如果缓存大小未指定或无效，设置一个默认值
	if maxCacheSize <= 0 {
		maxCacheSize = 100 // 默认缓存 100 页
	}

	return &Pager{
		file:         file,
		pageSize:     expectedPageSize, // 使用最终确定的页面大小
		numPages:     numPages,
		fileSize:     fileSize,
		pageCache:    make(map[PageID]Page, maxCacheSize), // 预分配缓存 map
		dirtyPages:   make(map[PageID]bool),
		maxCacheSize: maxCacheSize,
		closed:       false,
	}, nil
}

// AllocatePage 分配一个新的页面ID，并在需要时扩展底层文件。
// 返回新分配的 PageID。
// 注意：真实的数据库通常会有更复杂的空闲列表管理，这里简化为追加。
func (p *Pager) AllocatePage() (PageID, error) {
	p.mu.Lock() // 写锁定，修改 numPages 和 fileSize
	defer p.mu.Unlock()

	if p.closed {
		return 0, ErrPagerClosed
	}

	// 新页面的 ID 是当前的页面总数
	newPageID := p.numPages
	// 计算分配新页面后的预期文件大小
	newFileSize := int64(newPageID+1) * int64(p.pageSize)

	// 如果需要，扩展文件到新的大小
	// 在某些系统上（如 Linux 的 fallocate），预分配可能更高效
	if newFileSize > p.fileSize {
		// Truncate 会将文件扩展（如果 newFileSize 更大）并用零填充
		if err := p.file.Truncate(newFileSize); err != nil {
			// 尝试同步以确保之前的写入持久化
			_ = p.file.Sync()
			return 0, fmt.Errorf("为页面 %d 扩展文件至 %d 字节失败: %w", newPageID, newFileSize, err)
		}
		p.fileSize = newFileSize // 更新记录的文件大小
	}

	p.numPages++ // 增加总页面数

	// 可选：立即创建一个空页面并写入，确保空间被物理分配
	// 但通常依赖于第一次 WritePage 操作
	// emptyPage := make(Page, p.pageSize)
	// _, err := p.file.WriteAt(emptyPage, int64(newPageID)*int64(p.pageSize))
	// if err != nil { ... }

	return newPageID, nil
}

// ReadPage 从磁盘读取指定 ID 的页面。优先使用缓存。
// 返回页面的数据副本 (Page) 或错误。
func (p *Pager) ReadPage(pageID PageID) (Page, error) {
	p.mu.RLock() // 加读锁，检查缓存和状态
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPagerClosed
	}
	if pageID >= p.numPages {
		p.mu.RUnlock()
		return nil, fmt.Errorf("%w: 尝试读取页面 %d，但总页面数为 %d", ErrInvalidPageID, pageID, p.numPages)
	}

	// 1. 检查缓存
	if page, ok := p.pageCache[pageID]; ok {
		p.mu.RUnlock() // 缓存命中，释放读锁
		// 返回缓存页面的 *副本*，防止外部修改缓存内容
		pageCopy := make(Page, p.pageSize)
		copy(pageCopy, page)
		return pageCopy, nil
	}
	p.mu.RUnlock() // 缓存未命中，释放读锁，准备可能的磁盘 I/O

	// 2. 缓存未命中，从磁盘读取
	p.mu.Lock() // 加写锁，因为可能需要修改缓存 (添加新页) 或处理逐出
	defer p.mu.Unlock()

	// 获取写锁后，再次检查 Pager 是否已关闭或页面 ID 是否有效
	if p.closed {
		return nil, ErrPagerClosed
	}
	if pageID >= p.numPages { // 可能在等待锁期间文件被扩展了？（不太可能但防御性检查）
		return nil, fmt.Errorf("%w: 加锁后检查，尝试读取页面 %d，但总页面数为 %d", ErrInvalidPageID, pageID, p.numPages)
	}

	// 再次检查缓存（双重检查锁定模式），可能在等待写锁时其他 goroutine 已加载
	if page, ok := p.pageCache[pageID]; ok {
		pageCopy := make(Page, p.pageSize)
		copy(pageCopy, page)
		return pageCopy, nil
	}

	// --- 执行磁盘读取 ---
	pageData := make(Page, p.pageSize)          // 分配内存存储页面数据
	offset := int64(pageID) * int64(p.pageSize) // 计算文件偏移量

	n, err := p.file.ReadAt(pageData, offset)
	// 读取时遇到 EOF 只有在读取最后一个部分页时才可能发生，但我们的页面是固定大小的，所以 EOF 是意外的。
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("在偏移量 %d 读取页面 %d 失败: %w", offset, pageID, err)
	}
	// 检查读取的字节数是否符合预期
	if n != p.pageSize {
		// 如果读取字节不足，文件可能被意外截断或损坏
		return nil, fmt.Errorf("读取页面 %d 的字节数错误: 读取 %d, 期望 %d。文件可能已损坏。", pageID, n, p.pageSize)
	}

	// --- 校验和验证 ---
	// 跳过对全零页的校验和验证（可能是新分配但未初始化的页面）
	isAllZero := true
	for i := checksumSize; i < len(pageData); i++ { // 只检查数据部分，跳过校验和本身
		if pageData[i] != 0 {
			isAllZero = false
			break
		}
	}

	// 仅在非全零页面上验证校验和
	if !isAllZero {
		storedChecksum := readChecksum(pageData)                         // 读取页面中存储的校验和
		calculatedChecksum := calculateChecksum(pageData[checksumSize:]) // 计算数据部分的校验和
		if storedChecksum != calculatedChecksum {
			// 校验和不匹配，页面已损坏
			return nil, fmt.Errorf("%w: 页面 %d 校验和不匹配: 存储 %x, 计算 %x", ErrChecksumMismatch, pageID, storedChecksum, calculatedChecksum)
		}
	}

	// --- 添加到缓存 (如果需要，先逐出) ---
	if len(p.pageCache) >= p.maxCacheSize {
		evicted := p.evictPage() // 尝试逐出一个页面
		if !evicted && len(p.pageCache) >= p.maxCacheSize {
			// 逐出失败但缓存仍满，记录警告，可能影响性能
			fmt.Printf("警告: 页面缓存已满 (%d 页) 且逐出失败。无法缓存新读取的页面 %d。\n", p.maxCacheSize, pageID)
			// 在这种情况下，我们仍然返回从磁盘读取的数据，但不缓存它
			return pageData, nil
		}
	}

	// 将页面的 *副本* 存入缓存
	cachedPage := make(Page, p.pageSize)
	copy(cachedPage, pageData)
	p.pageCache[pageID] = cachedPage
	p.dirtyPages[pageID] = false // 刚从磁盘读取的页面是干净的

	// 向调用者返回读取数据的副本
	return pageData, nil
}

// WritePage 将内存中的页面数据写入 Pager 缓存，并标记为脏页。
// 实际的磁盘写入操作推迟到 Flush 或 Close 时进行。
// data 参数应该是完整的页面数据，函数会计算并覆盖其中的校验和。
func (p *Pager) WritePage(pageID PageID, data Page) error {
	p.mu.Lock() // 加写锁，修改缓存和 dirty 状态
	defer p.mu.Unlock()

	if p.closed {
		return ErrPagerClosed
	}
	if pageID >= p.numPages {
		return fmt.Errorf("%w: 尝试写入页面 %d，但总页面数为 %d", ErrInvalidPageID, pageID, p.numPages)
	}
	if len(data) != p.pageSize {
		return fmt.Errorf("尝试写入页面 %d 的数据大小 (%d) 与页面大小 (%d) 不符", pageID, len(data), p.pageSize)
	}

	// --- 计算并写入校验和 ---
	// 将校验和写入传入数据的副本，避免修改原始 data 切片
	dataToWrite := make(Page, p.pageSize)
	copy(dataToWrite, data)
	checksum := calculateChecksum(dataToWrite[checksumSize:]) // 计算数据部分的校验和
	writeChecksum(dataToWrite, checksum)                      // 将校验和写入副本的前部

	// --- 更新缓存 ---
	// 在添加新页面到缓存前检查容量，如果需要则逐出
	if _, exists := p.pageCache[pageID]; !exists && len(p.pageCache) >= p.maxCacheSize {
		evicted := p.evictPage() // 尝试逐出
		if !evicted && len(p.pageCache) >= p.maxCacheSize {
			// 逐出失败，缓存已满。写入失败。
			return fmt.Errorf("无法写入页面 %d：缓存已满 (%d 页) 且逐出失败", pageID, p.maxCacheSize)
		}
	}

	// 将包含校验和的数据副本存入缓存
	p.pageCache[pageID] = dataToWrite
	p.dirtyPages[pageID] = true // 标记页面为脏页

	return nil
}

// evictPage 是一个内部辅助函数，用于从缓存中逐出一个页面。(调用时需持有写锁)
// 返回是否成功逐出页面。
// 实现了一个简单的策略：优先逐出干净页，如果没有则逐出任意脏页（需要写回磁盘）。
// TODO: 替换为更优的 LRU (最近最少使用) 策略。
func (p *Pager) evictPage() bool {
	var evictID PageID = ^PageID(0) // 初始化为无效 ID
	var evictIsDirty bool = false

	// 1. 寻找一个干净的页面进行逐出
	for id, isDirty := range p.dirtyPages {
		if !isDirty {
			if _, existsInCache := p.pageCache[id]; existsInCache { // 确保页面仍在缓存中
				evictID = id
				evictIsDirty = false
				break
			} else {
				// dirtyPages 和 pageCache 不一致，清理脏页标记
				delete(p.dirtyPages, id)
			}
		}
	}

	// 2. 如果没有找到干净页面，则选择一个脏页面进行逐出 (需要写回)
	if evictID == ^PageID(0) {
		// 简单地选择第一个找到的脏页
		for id, isDirty := range p.dirtyPages {
			if isDirty {
				if _, existsInCache := p.pageCache[id]; existsInCache {
					evictID = id
					evictIsDirty = true
					break
				} else {
					delete(p.dirtyPages, id) // 清理不一致状态
				}
			}
		}
	}

	// 3. 如果找到了要逐出的页面 (无论是干净的还是脏的)
	if evictID != ^PageID(0) {
		pageDataToFlush := p.pageCache[evictID] // 获取页面数据
		delete(p.pageCache, evictID)            // 从缓存中移除
		delete(p.dirtyPages, evictID)           // 从脏页跟踪中移除

		// 如果逐出的是脏页，必须立即写回磁盘
		if evictIsDirty {
			// fmt.Printf("缓存逐出：正在刷新脏页 %d...\n", evictID)
			offset := int64(evictID) * int64(p.pageSize)
			n, err := p.file.WriteAt(pageDataToFlush, offset)
			if err != nil {
				// 严重错误：写入被逐出的脏页失败！可能导致数据丢失。
				fmt.Fprintf(os.Stderr, "严重错误：写入被逐出的脏页 %d 失败: %v。数据可能丢失。\n", evictID, err)
				// 尝试恢复？重新标记为脏？放入特殊队列？
				// 为简单起见，我们记录错误并认为逐出失败了，但页面已从缓存移除。
				// 可以考虑更健壮的策略，如将失败的页面放回 dirtyPages。
				return false // 逐出失败
			}
			if n != p.pageSize {
				fmt.Fprintf(os.Stderr, "严重错误：写入被逐出的脏页 %d 字节数不足 (%d/%d)。数据可能损坏。\n", evictID, n, p.pageSize)
				return false // 逐出失败
			}
			// 脏页写回后，是否需要 sync？通常不需要立即 sync，批量 sync 效率更高。
		}
		return true // 成功逐出
	}

	return false // 没有可逐出的页面
}

// FlushDirtyPages 将缓存中所有标记为脏的页面写入磁盘，并执行文件同步。
func (p *Pager) FlushDirtyPages() error {
	p.mu.Lock() // 加写锁，因为要进行磁盘写入并修改 dirty 状态
	defer p.mu.Unlock()

	if p.closed {
		return ErrPagerClosed
	}

	if len(p.dirtyPages) == 0 {
		return nil // 没有脏页需要刷新
	}

	// fmt.Printf("正在刷新 %d 个脏页...\n", len(p.dirtyPages))

	var firstError error                         // 记录遇到的第一个错误
	successfullyFlushed := make(map[PageID]bool) // 跟踪成功刷新的页面

	// 遍历所有标记为脏的页面
	for pageID, isDirty := range p.dirtyPages {
		if !isDirty { // 跳过非脏页（理论上不应出现在这里，但以防万一）
			continue
		}
		pageData, exists := p.pageCache[pageID]
		if !exists {
			// 脏页标记存在，但页面不在缓存中？这表示 Pager 内部状态不一致。
			fmt.Printf("警告：标记为脏的页面 %d 在刷新时未在缓存中找到。\n", pageID)
			delete(p.dirtyPages, pageID) // 清理无效的脏页标记
			continue
		}

		// --- 执行磁盘写入 ---
		offset := int64(pageID) * int64(p.pageSize)
		n, err := p.file.WriteAt(pageData, offset)

		if err != nil {
			err = fmt.Errorf("在偏移量 %d 写入页面 %d 失败: %w", offset, pageID, err)
			fmt.Println(err) // 记录错误
			if firstError == nil {
				firstError = err
			}
			// 写入失败，保留脏页状态，下次尝试
		} else if n != p.pageSize {
			err = fmt.Errorf("写入页面 %d 的字节数错误: 写入 %d, 期望 %d", pageID, n, p.pageSize)
			fmt.Println(err)
			if firstError == nil {
				firstError = err
			}
			// 写入不完整，保留脏页状态
		} else {
			// 写入成功，标记待清理
			successfullyFlushed[pageID] = true
		}
	}

	// 更新成功刷新的页面的脏状态
	for pageID := range successfullyFlushed {
		// 只有当页面仍然标记为脏时才更新（理论上总是如此，除非有并发问题）
		if _, ok := p.dirtyPages[pageID]; ok {
			p.dirtyPages[pageID] = false // 标记为不再脏
		}
	}

	// --- 同步文件 ---
	// 在所有脏页尝试写入后，执行一次文件系统同步，确保数据落盘。
	if err := p.file.Sync(); err != nil {
		err = fmt.Errorf("刷新后同步数据库文件失败: %w", err)
		fmt.Println(err)
		if firstError == nil {
			firstError = err
		}
	}

	// 清理 dirtyPages map 中所有不再脏的条目 (value 为 false)
	cleanedCount := 0
	for id, isDirty := range p.dirtyPages {
		if !isDirty {
			delete(p.dirtyPages, id)
			cleanedCount++
		}
	}

	// fmt.Printf("刷新完成。成功刷新 %d 个页面，清理 %d 个脏标记。\n", len(successfullyFlushed), cleanedCount)
	return firstError // 返回遇到的第一个错误
}

// Close 刷新所有脏页，执行最终同步，并关闭文件句柄。
func (p *Pager) Close() error {
	p.mu.Lock() // 加写锁，确保关闭操作的原子性
	defer p.mu.Unlock()

	if p.closed {
		return nil // 已经关闭
	}

	// fmt.Println("正在关闭页面管理器...")
	// 1. 刷新所有剩余的脏页
	flushErr := p.flushDirtyPagesInternal() // 使用内部版本，避免重复锁定

	// 2. 关闭文件句柄
	var closeErr error
	if p.file != nil {
		// 在关闭文件前最后一次同步（双重保险）
		syncErr := p.file.Sync()
		if syncErr != nil {
			fmt.Fprintf(os.Stderr, "关闭前最后同步文件出错: %v\n", syncErr)
			// 记录错误，但继续尝试关闭
			if flushErr == nil { // 如果刷新没有出错，将同步错误作为主要错误
				flushErr = syncErr
			}
		}

		closeErr = p.file.Close()
		if closeErr != nil {
			fmt.Fprintf(os.Stderr, "关闭数据库文件句柄出错: %v\n", closeErr)
		}
		p.file = nil // 清除文件句柄
	}

	// 3. 清理内部状态
	p.pageCache = nil // 释放缓存
	p.dirtyPages = nil
	p.closed = true // 标记为已关闭

	// fmt.Println("页面管理器已关闭。")

	// 返回遇到的第一个错误（刷新或关闭错误）
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// flushDirtyPagesInternal 是 FlushDirtyPages 的内部版本，假设调用者已持有写锁。
func (p *Pager) flushDirtyPagesInternal() error {
	if len(p.dirtyPages) == 0 {
		return nil
	}

	var firstError error
	successfullyFlushed := make(map[PageID]bool)

	for pageID, isDirty := range p.dirtyPages {
		if !isDirty {
			continue
		}
		pageData, exists := p.pageCache[pageID]
		if !exists {
			fmt.Printf("警告：(内部刷新) 标记为脏的页面 %d 未在缓存中找到。\n", pageID)
			delete(p.dirtyPages, pageID)
			continue
		}

		offset := int64(pageID) * int64(p.pageSize)
		n, err := p.file.WriteAt(pageData, offset)

		if err != nil {
			err = fmt.Errorf("(内部刷新) 在偏移量 %d 写入页面 %d 失败: %w", offset, pageID, err)
			fmt.Println(err)
			if firstError == nil {
				firstError = err
			}
		} else if n != p.pageSize {
			err = fmt.Errorf("(内部刷新) 写入页面 %d 的字节数错误: 写入 %d, 期望 %d", pageID, n, p.pageSize)
			fmt.Println(err)
			if firstError == nil {
				firstError = err
			}
		} else {
			successfullyFlushed[pageID] = true
		}
	}

	for pageID := range successfullyFlushed {
		if _, ok := p.dirtyPages[pageID]; ok {
			p.dirtyPages[pageID] = false
		}
	}

	// 注意：内部刷新不执行 Sync，由外部调用者（如 Close 或 FlushDirtyPages）负责。

	// 清理不再脏的条目
	for id, isDirty := range p.dirtyPages {
		if !isDirty {
			delete(p.dirtyPages, id)
		}
	}
	return firstError
}

// ==========================================================================
// 校验和工具
// ==========================================================================

// calculateChecksum 使用 CRC32 IEEE 表计算数据字节片的校验和。
func calculateChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// writeChecksum 将 uint32 校验和以小端序写入页面数据的开头（前 checksumSize 字节）。
func writeChecksum(page Page, checksum uint32) {
	if len(page) >= checksumSize {
		binary.LittleEndian.PutUint32(page[0:checksumSize], checksum)
	}
}

// readChecksum 从页面数据的开头读取 uint32 校验和（小端序）。
func readChecksum(page Page) uint32 {
	if len(page) >= checksumSize {
		return binary.LittleEndian.Uint32(page[0:checksumSize])
	}
	// 如果页面长度不足，返回 0 或错误？返回 0 可能导致校验和误判。
	// 更好的做法是确保页面长度始终足够，并在 pager 中处理长度错误。
	return 0
}

// ==========================================================================
// B+树节点 (Node)
// ==========================================================================

// Item 代表存储在叶子节点中的键值对。
type Item struct {
	Key   []byte
	Value []byte
}

// Node 代表 B+树中的一个节点，存储在一个页面内。
type Node struct {
	pageID   PageID   // 本节点存储在哪个页面
	isLeaf   bool     // 是否为叶子节点
	numKeys  uint16   // 当前节点中的键数量
	items    []Item   // 存储的项（叶子节点包含键值，内部节点只用键）
	children []PageID // 指向子节点的页面ID列表（仅内部节点使用）
	nextLeaf PageID   // 指向下一个叶子节点的页面ID（仅叶子节点使用，0表示无）
	prevLeaf PageID   // 指向前一个叶子节点的页面ID（仅叶子节点使用，0表示无）

	// --- 瞬态字段 (不直接序列化，由 BTree 管理) ---
	pager *Pager       // 对页面管理器的引用
	btree *BTree       // 对所属 B+树的引用
	dirty bool         // 标记节点在内存中是否被修改过
	mu    sync.RWMutex // 对本节点的读写锁 (用于更细粒度的并发控制，本示例中未使用，依赖 BTree 级锁)
}

// maxKeys 返回节点理论上能容纳的最大键数 (2t - 1)。
func (n *Node) maxKeys() int {
	// B+树的度(t) = minKeys + 1
	// maxKeys = 2*t - 1 = 2*(n.btree.degree) - 1
	return 2*n.btree.degree - 1
}

// minKeys 返回节点需要保持的最小键数 (t - 1)，根节点除外。
func (n *Node) minKeys() int {
	// t - 1 = n.btree.degree - 1
	return n.btree.degree - 1
}

// isFull 检查节点是否已满（键数量达到最大值）。
func (n *Node) isFull() bool {
	return int(n.numKeys) >= n.maxKeys()
}

// --- 节点序列化/反序列化 ---

// estimateNodeSize 估算节点序列化后的大小，用于检查是否超限。
// 这是一个估算，实际大小取决于键值数据的具体内容。
func (n *Node) estimateNodeSize() int {
	size := checksumSize + nodeHeaderBaseSize // 校验和 + 基本头部
	if n.isLeaf {
		size += pagePointerSize         // nextLeaf 指针
		size += leafNodePointerAreaSize // keys/values 偏移量指针
		for _, item := range n.items {
			size += 2 + len(item.Key)   // key 长度 + key 数据
			size += 2 + len(item.Value) // value 长度 + value 数据
		}
	} else {
		size += internalNodePointerAreaSize       // keys/children 偏移量指针
		size += len(n.children) * pagePointerSize // 子节点指针
		for _, item := range n.items {
			size += 2 + len(item.Key) // key 长度 + key 数据
		}
	}
	return size
}

// serialize 将 Node 对象序列化为字节切片 (Page)。
// 这是核心且复杂的部分，需要精确计算偏移量和写入数据。
func (n *Node) serialize(pageSize int) (Page, error) {
	// 预估大小，如果明显超限则提前失败
	// estimated := n.estimateNodeSize()
	// if estimated > pageSize {
	// 	return nil, fmt.Errorf("%w: 节点 %d 预估大小 %d 超过页面大小 %d", ErrDataTooLarge, n.pageID, estimated, pageSize)
	// }

	pageData := make(Page, pageSize) // 创建页面缓冲区
	offset := 0                      // 当前写入偏移量

	// 1. 校验和区域 (占位，由 Pager.WritePage 填充)
	offset += checksumSize

	// 2. 节点头部
	headerStart := offset
	// isLeaf (1 byte)
	if n.isLeaf {
		pageData[offset] = 1
	} else {
		pageData[offset] = 0
	}
	offset++
	// numKeys (2 bytes, LittleEndian)
	if int(n.numKeys) > n.maxKeys() { // 基本健全性检查
		return nil, fmt.Errorf("序列化错误 (页 %d): numKeys %d 超出 maxKeys %d", n.pageID, n.numKeys, n.maxKeys())
	}
	binary.LittleEndian.PutUint16(pageData[offset:offset+2], n.numKeys)
	offset += 2
	// 保留/填充 (1 byte) - 使得头部固定为 4 字节
	pageData[offset] = 0
	offset++
	if offset-headerStart != nodeHeaderBaseSize {
		panic("内部错误：nodeHeaderBaseSize 计算不匹配") // 理论上不应发生
	}

	// 3. 指针区域 (Pointer Area)
	//    - 叶子节点: nextLeaf(8), keysOffset(2), valuesOffset(2) = 12 字节
	//    - 内部节点: keysOffset(2), childrenOffset(2) = 4 字节
	var keysOffsetFieldPos, valuesOrChildrenOffsetFieldPos int
	if n.isLeaf {
		// nextLeaf (8 bytes, LittleEndian)
		binary.LittleEndian.PutUint64(pageData[offset:offset+pagePointerSize], uint64(n.nextLeaf))
		offset += pagePointerSize

		// prevLeaf (8 bytes, LittleEndian)
		binary.LittleEndian.PutUint64(pageData[offset:offset+pagePointerSize], uint64(n.prevLeaf))
		offset += pagePointerSize

		// keysOffset 存储位置 (2 bytes)
		keysOffsetFieldPos = offset
		offset += 2
		// valuesOffset 存储位置 (2 bytes)
		valuesOrChildrenOffsetFieldPos = offset
		offset += 2
	} else { // 内部节点
		// keysOffset 存储位置 (2 bytes)
		keysOffsetFieldPos = offset
		offset += 2
		// childrenOffset 存储位置 (2 bytes)
		valuesOrChildrenOffsetFieldPos = offset
		offset += 2
	}

	// --- 数据区域 (Data Area) ---
	// 数据从固定头部和指针区域之后开始写入
	dataStartOffset := offset

	// 4. 子节点指针 (仅内部节点)
	childrenStartOffset := dataStartOffset // 子节点数据开始的位置
	if !n.isLeaf {
		childCount := int(n.numKeys) + 1
		if len(n.children) != childCount {
			return nil, fmt.Errorf("序列化错误 (页 %d): 内部节点有 %d 个键但子节点数 %d 不等于 %d", n.pageID, n.numKeys, len(n.children), childCount)
		}
		requiredSpace := childCount * pagePointerSize
		if dataStartOffset+requiredSpace > pageSize {
			return nil, fmt.Errorf("%w: 序列化内部节点 %d 时子节点指针空间不足 (需要 %d, 可用 %d)", ErrDataTooLarge, n.pageID, requiredSpace, pageSize-dataStartOffset)
		}
		currentDataOffset := dataStartOffset
		for _, childID := range n.children {
			binary.LittleEndian.PutUint64(pageData[currentDataOffset:currentDataOffset+pagePointerSize], uint64(childID))
			currentDataOffset += pagePointerSize
		}
		// 更新数据区域的起始偏移量
		dataStartOffset = currentDataOffset
		// 将 children 数据区的实际起始偏移量写入指针区
		binary.LittleEndian.PutUint16(pageData[valuesOrChildrenOffsetFieldPos:valuesOrChildrenOffsetFieldPos+2], uint16(childrenStartOffset))
	}

	// 5. 键 (Keys)
	keysStartOffset := dataStartOffset // 键数据开始的位置
	currentDataOffset := keysStartOffset
	for i, item := range n.items {
		keyLen := len(item.Key)
		requiredSpace := 2 + keyLen // 长度前缀(2) + 数据
		if currentDataOffset+requiredSpace > pageSize {
			return nil, fmt.Errorf("%w: 序列化节点 %d 时键 %d (len %d) 空间不足 (当前偏移 %d, 页面大小 %d)", ErrDataTooLarge, n.pageID, i, keyLen, currentDataOffset, pageSize)
		}
		// 写入键长度 (2 bytes, LittleEndian)
		binary.LittleEndian.PutUint16(pageData[currentDataOffset:currentDataOffset+2], uint16(keyLen))
		currentDataOffset += 2
		// 写入键数据
		copy(pageData[currentDataOffset:], item.Key)
		currentDataOffset += keyLen
	}
	// 更新数据区域的起始偏移量
	dataStartOffset = currentDataOffset
	// 将 keys 数据区的实际起始偏移量写入指针区
	binary.LittleEndian.PutUint16(pageData[keysOffsetFieldPos:keysOffsetFieldPos+2], uint16(keysStartOffset))

	// 6. 值 (Values) (仅叶子节点)
	if n.isLeaf {
		valuesStartOffset := dataStartOffset // 值数据开始的位置
		currentDataOffset = valuesStartOffset
		for i, item := range n.items {
			valLen := len(item.Value)
			requiredSpace := 2 + valLen // 长度前缀(2) + 数据
			if currentDataOffset+requiredSpace > pageSize {
				return nil, fmt.Errorf("%w: 序列化节点 %d 时值 %d (len %d) 空间不足 (当前偏移 %d, 页面大小 %d)", ErrDataTooLarge, n.pageID, i, valLen, currentDataOffset, pageSize)
			}
			// 写入值长度 (2 bytes, LittleEndian)
			binary.LittleEndian.PutUint16(pageData[currentDataOffset:currentDataOffset+2], uint16(valLen))
			currentDataOffset += 2
			// 写入值数据
			copy(pageData[currentDataOffset:], item.Value)
			currentDataOffset += valLen
		}
		// 更新数据区域的起始偏移量
		dataStartOffset = currentDataOffset
		// 将 values 数据区的实际起始偏移量写入指针区
		binary.LittleEndian.PutUint16(pageData[valuesOrChildrenOffsetFieldPos:valuesOrChildrenOffsetFieldPos+2], uint16(valuesStartOffset))
	}

	// 检查最终偏移量是否超限
	if dataStartOffset > pageSize {
		return nil, fmt.Errorf("序列化错误 (页 %d): 最终数据偏移量 %d 超出页面大小 %d", n.pageID, dataStartOffset, pageSize)
	}

	// 将剩余空间清零（可选，但有助于调试和一致性）
	// for i := dataStartOffset; i < pageSize; i++ {
	// 	pageData[i] = 0
	// }

	return pageData, nil
}

// deserializeNode 将字节切片 (Page) 反序列化为 Node 对象。
func deserializeNode(pageData Page, pageID PageID, pager *Pager, btree *BTree) (*Node, error) {
	pageSize := len(pageData)
	minRequiredSize := checksumSize + nodeHeaderBaseSize // 最小需要的大小
	if pageSize < minRequiredSize {
		return nil, fmt.Errorf("反序列化错误 (页 %d): 页面数据过短 (%d 字节)，至少需要 %d 字节", pageID, pageSize, minRequiredSize)
	}
	offset := 0

	// 1. 校验和 (由 Pager.ReadPage 验证，这里跳过)
	offset += checksumSize

	// 2. 节点头部
	headerStart := offset
	isLeafByte := pageData[offset]
	offset++
	numKeys := binary.LittleEndian.Uint16(pageData[offset : offset+2])
	offset += 2
	// 跳过保留/填充字节
	offset++
	if offset-headerStart != nodeHeaderBaseSize {
		panic("内部错误：反序列化时 nodeHeaderBaseSize 不匹配")
	}

	node := &Node{
		pageID:   pageID,
		isLeaf:   isLeafByte == 1,
		numKeys:  numKeys,
		items:    make([]Item, numKeys), // 根据 numKeys 初始化切片大小
		children: nil,                   // 稍后初始化（如果需要）
		nextLeaf: 0,                     // 稍后初始化（如果需要）
		pager:    pager,
		btree:    btree,
		dirty:    false, // 刚读取的节点是干净的
	}

	// 3. 指针区域和数据读取
	var keysOffset, valuesOrChildrenOffset int
	var dataStartOffset int // 指针区域结束后的数据起始偏移量

	if node.isLeaf {
		minRequiredSize += pagePointerSize + leafNodePointerAreaSize // 加上叶子节点指针区大小
		if pageSize < minRequiredSize {
			return nil, fmt.Errorf("反序列化错误 (页 %d): 叶子节点数据过短 (%d 字节)", pageID, pageSize)
		}
		// 读取 nextLeaf
		node.nextLeaf = PageID(binary.LittleEndian.Uint64(pageData[offset : offset+pagePointerSize]))
		offset += pagePointerSize

		// 读取 prevLeaf
		node.prevLeaf = PageID(binary.LittleEndian.Uint64(pageData[offset : offset+pagePointerSize]))
		offset += pagePointerSize

		// 读取 keysOffset 和 valuesOffset 的值
		keysOffset = int(binary.LittleEndian.Uint16(pageData[offset : offset+2]))
		offset += 2
		valuesOrChildrenOffset = int(binary.LittleEndian.Uint16(pageData[offset : offset+2]))
		offset += 2
		dataStartOffset = offset // 记录指针区域结束的位置

		// --- 叶子节点：读取 Keys ---
		currentDataOffset := keysOffset
		// 健全性检查：keysOffset 必须在指针区之后，并且在 valuesOffset 之前或等于它
		if keysOffset < dataStartOffset || keysOffset > valuesOrChildrenOffset || keysOffset > pageSize {
			return nil, fmt.Errorf("反序列化错误 (页 %d): 无效的 keysOffset %d (数据起始 %d, valuesOffset %d)", pageID, keysOffset, dataStartOffset, valuesOrChildrenOffset)
		}
		for i := 0; i < int(numKeys); i++ {
			// 读取键长度
			if currentDataOffset+2 > valuesOrChildrenOffset || currentDataOffset+2 > pageSize {
				return nil, fmt.Errorf("反序列化错误 (页 %d): 读取键 %d 长度时溢出 (偏移 %d)", pageID, i, currentDataOffset)
			}
			keyLen := int(binary.LittleEndian.Uint16(pageData[currentDataOffset : currentDataOffset+2]))
			currentDataOffset += 2
			// 读取键数据
			if currentDataOffset+keyLen > valuesOrChildrenOffset || currentDataOffset+keyLen > pageSize {
				return nil, fmt.Errorf("反序列化错误 (页 %d): 读取键 %d (len %d) 数据时溢出 (偏移 %d)", pageID, i, keyLen, currentDataOffset)
			}
			node.items[i].Key = make([]byte, keyLen)
			copy(node.items[i].Key, pageData[currentDataOffset:currentDataOffset+keyLen])
			currentDataOffset += keyLen
		}
		// 健全性检查：读取完所有 key 后，偏移量应等于 valuesOffset
		if currentDataOffset != valuesOrChildrenOffset {
			fmt.Printf("警告 (页 %d): 读取键后偏移量 %d 与 valuesOffset %d 不匹配\n", pageID, currentDataOffset, valuesOrChildrenOffset)
			// 可以选择返回错误或继续，取决于容错策略
			// return nil, fmt.Errorf("反序列化错误 (页 %d): 读取键后偏移量 %d 与 valuesOffset %d 不匹配", pageID, currentDataOffset, valuesOrChildrenOffset)
		}

		// --- 叶子节点：读取 Values ---
		currentDataOffset = valuesOrChildrenOffset
		// 健全性检查：valuesOffset 必须在 keys 数据之后，且在页面范围内
		if valuesOrChildrenOffset < keysOffset || valuesOrChildrenOffset > pageSize {
			return nil, fmt.Errorf("反序列化错误 (页 %d): 无效的 valuesOffset %d (keysOffset %d)", pageID, valuesOrChildrenOffset, keysOffset)
		}
		for i := 0; i < int(numKeys); i++ {
			// 读取值长度
			if currentDataOffset+2 > pageSize {
				return nil, fmt.Errorf("反序列化错误 (页 %d): 读取值 %d 长度时溢出 (偏移 %d)", pageID, i, currentDataOffset)
			}
			valLen := int(binary.LittleEndian.Uint16(pageData[currentDataOffset : currentDataOffset+2]))
			currentDataOffset += 2
			// 读取值数据
			if currentDataOffset+valLen > pageSize {
				return nil, fmt.Errorf("反序列化错误 (页 %d): 读取值 %d (len %d) 数据时溢出 (偏移 %d)", pageID, i, valLen, currentDataOffset)
			}
			node.items[i].Value = make([]byte, valLen)
			copy(node.items[i].Value, pageData[currentDataOffset:currentDataOffset+valLen])
			currentDataOffset += valLen
		}

	} else { // 内部节点
		minRequiredSize += internalNodePointerAreaSize // 加上内部节点指针区大小
		if pageSize < minRequiredSize {
			return nil, fmt.Errorf("反序列化错误 (页 %d): 内部节点数据过短 (%d 字节)", pageID, pageSize)
		}
		// 读取 keysOffset 和 childrenOffset 的值
		keysOffset = int(binary.LittleEndian.Uint16(pageData[offset : offset+2]))
		offset += 2
		valuesOrChildrenOffset = int(binary.LittleEndian.Uint16(pageData[offset : offset+2])) // 这里存的是 childrenOffset
		offset += 2
		dataStartOffset = offset // 记录指针区域结束的位置

		childrenOffset := valuesOrChildrenOffset // 重命名以便理解

		// --- 内部节点：读取 Children ---
		childCount := int(numKeys) + 1
		node.children = make([]PageID, childCount)
		currentDataOffset := childrenOffset
		requiredChildrenSpace := childCount * pagePointerSize
		// 健全性检查：childrenOffset 必须在指针区之后，并且 children 数据必须在 keysOffset 之前结束
		if childrenOffset < dataStartOffset || childrenOffset+requiredChildrenSpace > keysOffset || childrenOffset+requiredChildrenSpace > pageSize {
			return nil, fmt.Errorf("反序列化错误 (页 %d): 无效的 childrenOffset %d 或空间不足 (数据起始 %d, keysOffset %d, 需要 %d)", pageID, childrenOffset, dataStartOffset, keysOffset, requiredChildrenSpace)
		}
		for i := 0; i < childCount; i++ {
			node.children[i] = PageID(binary.LittleEndian.Uint64(pageData[currentDataOffset : currentDataOffset+pagePointerSize]))
			currentDataOffset += pagePointerSize
		}
		// 健全性检查：读取完所有 children 后，偏移量应等于 keysOffset
		if currentDataOffset != keysOffset {
			fmt.Printf("警告 (页 %d): 读取子节点后偏移量 %d 与 keysOffset %d 不匹配\n", pageID, currentDataOffset, keysOffset)
			// return nil, fmt.Errorf("反序列化错误 (页 %d): 读取子节点后偏移量 %d 与 keysOffset %d 不匹配", pageID, currentDataOffset, keysOffset)
		}

		// --- 内部节点：读取 Keys ---
		currentDataOffset = keysOffset
		// 健全性检查：keysOffset 必须在 children 数据之后，且在页面范围内
		if keysOffset < childrenOffset+requiredChildrenSpace || keysOffset > pageSize {
			return nil, fmt.Errorf("反序列化错误 (页 %d): 无效的 keysOffset %d (children 结束于 %d)", pageID, keysOffset, childrenOffset+requiredChildrenSpace)
		}
		for i := 0; i < int(numKeys); i++ {
			// 读取键长度
			if currentDataOffset+2 > pageSize {
				return nil, fmt.Errorf("反序列化错误 (页 %d): 读取内部键 %d 长度时溢出 (偏移 %d)", pageID, i, currentDataOffset)
			}
			keyLen := int(binary.LittleEndian.Uint16(pageData[currentDataOffset : currentDataOffset+2]))
			currentDataOffset += 2
			// 读取键数据
			if currentDataOffset+keyLen > pageSize {
				return nil, fmt.Errorf("反序列化错误 (页 %d): 读取内部键 %d (len %d) 数据时溢出 (偏移 %d)", pageID, i, keyLen, currentDataOffset)
			}
			node.items[i].Key = make([]byte, keyLen)
			copy(node.items[i].Key, pageData[currentDataOffset:currentDataOffset+keyLen])
			currentDataOffset += keyLen
			// 内部节点的值通常不存储或为 nil
			node.items[i].Value = nil
		}
	}

	// 最终检查：反序列化的项数是否与头部记录的 numKeys 匹配
	if len(node.items) != int(numKeys) {
		return nil, fmt.Errorf("反序列化错误 (页 %d): 头部 numKeys (%d) 与实际读取的项数 (%d) 不匹配", pageID, numKeys, len(node.items))
	}

	// 基本的 B+树属性检查 (可选，但有帮助)
	if !node.isLeaf && len(node.children) != int(node.numKeys)+1 {
		return nil, fmt.Errorf("反序列化错误 (页 %d): 内部节点键数 %d 与子节点数 %d 不匹配 (应为 %d)", pageID, node.numKeys, len(node.children), int(node.numKeys)+1)
	}
	// B+树中，叶子节点没有 children
	if node.isLeaf && node.children != nil {
		return nil, fmt.Errorf("反序列化错误 (页 %d): 叶子节点不应有子节点指针", pageID)
	}

	return node, nil
}

// ==========================================================================
// 元数据 (MetaData)
// ==========================================================================

// MetaData 定义了存储在页面 0 的数据库元信息。
type MetaData struct {
	MagicNumber uint32 // 标识数据库文件类型的幻数
	RootPageID  PageID // B+树根节点的页面 ID
	PageSize    uint32 // 数据库创建时使用的页面大小
	Degree      uint32 // B+树的度 (t)
	// 未来可以添加更多字段：如空闲列表头指针、总条目数、版本号等
}

// metaDataFixedSize 计算 MetaData 结构序列化后的固定大小。
// 需要与 MetaData 结构字段保持同步！
const metaDataFixedSize = 4 + 8 + 4 + 4 // Magic(4) + RootID(8) + PageSize(4) + Degree(4)

// serialize 将 MetaData 对象序列化为 Page 数据（填充到页面大小）。
func (m *MetaData) serialize(pageSize int) (Page, error) {
	requiredSize := checksumSize + metaDataFixedSize
	if pageSize < requiredSize {
		return nil, fmt.Errorf("元数据序列化错误：页面大小 %d 小于所需最小大小 %d", pageSize, requiredSize)
	}
	pageData := make(Page, pageSize) // 创建完整页面大小的缓冲区

	// 使用 bytes.Buffer 可以简化写入过程
	// 从校验和之后开始写入 (pageData[checksumSize:])
	buf := bytes.NewBuffer(pageData[checksumSize:checksumSize]) // 容量为 pageSize - checksumSize

	// 按顺序以小端序写入字段
	if err := binary.Write(buf, binary.LittleEndian, m.MagicNumber); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, m.RootPageID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, m.PageSize); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, m.Degree); err != nil {
		return nil, err
	}
	// ... 如果添加了新字段，在此处继续写入 ...

	// 检查写入的数据量是否超出预期（理论上不应发生，因为 buf 有容量限制）
	if buf.Len() > metaDataFixedSize {
		panic(fmt.Sprintf("内部错误：元数据序列化写入 %d 字节，超过预期的 %d 字节", buf.Len(), metaDataFixedSize))
	}

	// 将 buffer 的内容写回到 pageData 切片中 (实际上 NewBuffer 已经在操作底层数组了)
	// 校验和由 Pager.WritePage 负责写入 pageData[0:checksumSize]

	return pageData, nil
}

// deserializeMetaData 从页面数据反序列化 MetaData 对象。
func deserializeMetaData(pageData Page) (*MetaData, error) {
	pageSize := len(pageData)
	requiredSize := checksumSize + metaDataFixedSize
	if pageSize < requiredSize {
		return nil, fmt.Errorf("元数据反序列化错误：页面数据过短 (%d 字节)，至少需要 %d 字节", pageSize, requiredSize)
	}

	// 从校验和之后开始读取 (pageData[checksumSize:])
	buf := bytes.NewReader(pageData[checksumSize:])
	meta := &MetaData{}

	// 按顺序以小端序读取字段
	if err := binary.Read(buf, binary.LittleEndian, &meta.MagicNumber); err != nil {
		return nil, err
	}
	// 验证幻数
	if meta.MagicNumber != magicNumber {
		return nil, fmt.Errorf("%w: 期望 %x, 得到 %x", ErrInvalidMagicNumber, magicNumber, meta.MagicNumber)
	}
	if err := binary.Read(buf, binary.LittleEndian, &meta.RootPageID); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &meta.PageSize); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &meta.Degree); err != nil {
		return nil, err
	}
	// ... 如果添加了新字段，在此处继续读取 ...

	// 可以在这里添加对元数据值的进一步验证，例如 PageSize > 0, Degree > 1 等

	return meta, nil
}

// ==========================================================================
// B+树 (BTree)
// ==========================================================================

// BTree 是 B+树数据结构的主要管理器。
type BTree struct {
	pager      *Pager       // 关联的页面管理器
	rootPageID PageID       // 当前根节点的页面 ID
	degree     int          // B+树的度 (t)，决定节点容量
	metaDirty  bool         // 标记元数据（主要是 rootPageID 或 degree）是否已更改且需要保存
	mu         sync.RWMutex // 保护 BTree 结构性变化（如 rootPageID 更新）的读写锁
}

// NewBTree 创建一个新的 BTree 实例。
// 如果数据库文件已存在，则加载它；否则，初始化一个新的数据库文件。
func NewBTree(filename string) (*BTree, error) {
	// 使用默认页面大小和缓存大小创建 Pager
	pager, err := NewPager(filename, DefaultPageSize, 1000) // 缓存 1000 页
	if err != nil {
		return nil, err
	}

	btree := &BTree{
		pager: pager,
		// degree 和 rootPageID 将从文件加载或在新文件时初始化
		metaDirty: false,
	}

	if pager.numPages == 0 {
		// --- 初始化新的数据库文件 ---
		fmt.Println("初始化新的数据库文件...")
		btree.mu.Lock() // 加写锁进行初始化

		// 1. 设置 B+树的度 (t)
		btree.degree = defaultMinKeysPerNode + 1 // t = minKeys + 1

		// 2. 分配元数据页 (Page 0)
		metaPIDAllocated, err := pager.AllocatePage()
		if err != nil || metaPIDAllocated != metaPageID {
			pager.Close() // 分配失败，清理并返回
			btree.mu.Unlock()
			return nil, fmt.Errorf("初始化错误：分配元数据页 %d 失败: %w", metaPageID, err)
		}

		// 3. 分配初始根节点页 (它将是一个叶子节点)
		rootPID, err := pager.AllocatePage()
		if err != nil {
			pager.Close()
			btree.mu.Unlock()
			return nil, fmt.Errorf("初始化错误：分配初始根页面失败: %w", err)
		}
		btree.rootPageID = rootPID // 设置 BTree 的根节点 ID

		// 4. 创建空的根节点 (初始时是叶子)
		rootNode := &Node{
			pageID:   rootPID,
			isLeaf:   true,
			numKeys:  0,
			items:    []Item{}, // 空的 items
			children: nil,      // 叶子节点无 children
			nextLeaf: 0,        // 初始时没有下一个叶子
			pager:    pager,
			btree:    btree,
			dirty:    true, // 新节点需要写入
		}

		// 5. 将新的根节点写入其页面
		err = btree.putNode(rootNode) // putNode 包含序列化和调用 pager.WritePage
		if err != nil {
			pager.Close()
			btree.mu.Unlock()
			return nil, fmt.Errorf("初始化错误：写入初始根节点 %d 失败: %w", rootPID, err)
		}

		// 6. 创建元数据并写入元数据页 (Page 0)
		btree.metaDirty = true         // 标记元数据已更改
		err = btree.saveMetaInternal() // 使用内部版本，因为已持有锁
		if err != nil {
			pager.Close()
			btree.mu.Unlock()
			return nil, fmt.Errorf("初始化错误：写入初始元数据失败: %w", err)
		}

		// 初始化完成后解锁
		btree.mu.Unlock()

		// 7. 刷新 Pager 确保初始化持久化 (可在 BTree 锁之外进行)
		err = pager.FlushDirtyPages()
		if err != nil {
			// 警告：刷新失败可能意味着初始化未完全持久化
			fmt.Fprintf(os.Stderr, "警告：初始化后刷新页面失败: %v\n", err)
			// 考虑是否需要关闭并返回错误
			// pager.Close()
			// return nil, fmt.Errorf("初始化后刷新页面失败: %w", err)
		}
		fmt.Println("数据库初始化完成。")

	} else {
		// --- 加载现有的数据库文件 ---
		fmt.Println("加载现有的数据库文件...")
		// 1. 读取元数据页 (Page 0)
		metaPageData, err := pager.ReadPage(metaPageID)
		if err != nil {
			pager.Close()
			return nil, fmt.Errorf("%w: 读取元数据页 %d 失败: %w", ErrMetaReadFailed, metaPageID, err)
		}

		// 2. 反序列化元数据
		meta, err := deserializeMetaData(metaPageData)
		if err != nil {
			pager.Close()
			return nil, fmt.Errorf("%w: 反序列化元数据失败: %w", ErrMetaReadFailed, err)
		}

		// 3. 验证元数据
		//    - MagicNumber 已在 deserializeMetaData 中验证
		//    - 检查文件中的 PageSize 是否与 Pager 配置一致
		if meta.PageSize != uint32(pager.pageSize) {
			// Pager 的 NewPager 应该已经处理了不匹配并使用了文件中的 pageSize，这里是双重检查
			fmt.Printf("警告：元数据中的页面大小 %d 与 Pager 实例的页面大小 %d 不一致 (应已被 NewPager 修正)。\n", meta.PageSize, pager.pageSize)
			// 如果未被修正，则需要报错并关闭
			if int(meta.PageSize) != pager.pageSize {
				pager.Close()
				return nil, fmt.Errorf("%w: 文件使用的页面大小 %d 与 Pager 配置 %d 冲突", ErrInvalidPageSize, meta.PageSize, pager.pageSize)
			}
		}
		//    - 检查 Degree 是否有效
		if meta.Degree < 2 { // B+树的度 t 必须 >= 2
			pager.Close()
			return nil, fmt.Errorf("元数据错误：无效的 B+树度 %d (必须 >= 2)", meta.Degree)
		}
		//    - 检查 RootPageID 是否在有效范围内
		if meta.RootPageID == 0 || meta.RootPageID >= pager.numPages {
			pager.Close()
			return nil, fmt.Errorf("元数据错误：无效的根页面ID %d (总页面数 %d)", meta.RootPageID, pager.numPages)
		}

		// 4. 从元数据设置 BTree 字段
		btree.mu.Lock() // 加锁设置内部状态
		btree.rootPageID = meta.RootPageID
		btree.degree = int(meta.Degree)
		btree.mu.Unlock()

		fmt.Printf("数据库已加载。根页面: %d, 度: %d, 页面大小: %d\n", btree.rootPageID, btree.degree, meta.PageSize)
	}

	return btree, nil
}

// getNode 从 Pager 读取页面数据，并将其反序列化为 Node 对象。
func (bt *BTree) getNode(pageID PageID) (*Node, error) {
	if pageID == 0 { // Page 0 是元数据页
		return nil, fmt.Errorf("%w: 尝试获取元数据页 %d 作为节点", ErrInvalidPageID, pageID)
	}
	// 从 Pager 获取页面数据
	pageData, err := bt.pager.ReadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("%w: Pager 读取页面 %d 失败: %w", ErrNodeReadFailed, pageID, err)
	}

	// 反序列化为 Node
	node, err := deserializeNode(pageData, pageID, bt.pager, bt)
	if err != nil {
		return nil, fmt.Errorf("%w: 从页面 %d 反序列化节点失败: %w", ErrNodeReadFailed, pageID, err)
	}
	return node, nil
}

// putNode 将 Node 对象序列化，并通过 Pager 将其写入缓存（标记为脏页）。
func (bt *BTree) putNode(node *Node) error {
	if node == nil {
		return errors.New("尝试写入 nil 节点")
	}
	if node.pageID == 0 { // 不能写入元数据页
		return fmt.Errorf("%w: 尝试将节点写入无效页面ID %d", ErrInvalidPageID, node.pageID)
	}

	// 序列化 Node -> Page
	pageData, err := node.serialize(bt.pager.pageSize)
	if err != nil {
		return fmt.Errorf("%w: 序列化页面 %d 的节点失败: %w", ErrNodeWriteFailed, node.pageID, err)
	}

	// 通过 Pager 写入（到缓存）
	err = bt.pager.WritePage(node.pageID, pageData)
	if err != nil {
		return fmt.Errorf("%w: Pager 写入页面 %d 失败: %w", ErrNodeWriteFailed, node.pageID, err)
	}
	// 成功写入缓存后，标记节点在内存中是干净的（相对于缓存中的状态）
	// 注意：这并不意味着它已落盘
	node.dirty = false
	return nil
}

// saveMeta 保存当前的元数据（如果 metaDirty 为 true）。
func (bt *BTree) saveMeta() error {
	bt.mu.Lock() // 加写锁保护 metaDirty 和 rootPageID 的读取
	defer bt.mu.Unlock()
	return bt.saveMetaInternal()
}

// saveMetaInternal 是 saveMeta 的内部版本，假设调用者已持有写锁。
func (bt *BTree) saveMetaInternal() error {
	if !bt.metaDirty {
		return nil // 元数据未更改，无需保存
	}

	// 创建包含当前状态的 MetaData 对象
	meta := &MetaData{
		MagicNumber: magicNumber,
		RootPageID:  bt.rootPageID,
		PageSize:    uint32(bt.pager.pageSize),
		Degree:      uint32(bt.degree),
		// ... 如果添加了新字段 ...
	}

	// 序列化 MetaData -> Page
	metaPageData, err := meta.serialize(bt.pager.pageSize)
	if err != nil {
		return fmt.Errorf("%w: 序列化元数据失败: %w", ErrMetaWriteFailed, err)
	}

	// 通过 Pager 写入元数据页 (Page 0)
	err = bt.pager.WritePage(metaPageID, metaPageData)
	if err != nil {
		// 写入失败，保持 metaDirty 状态，以便下次尝试
		return fmt.Errorf("%w: Pager 写入元数据页 %d 失败: %w", ErrMetaWriteFailed, metaPageID, err)
	}

	// 成功写入 Pager 缓存后，标记元数据为干净
	bt.metaDirty = false
	// fmt.Println("元数据已保存到 Pager 缓存。")
	return nil
}

// Close 安全地关闭 BTree，包括保存元数据和关闭 Pager。
func (bt *BTree) Close() error {
	// fmt.Println("正在关闭 BTree...")
	// 1. 保存最终的元数据（如果需要）
	metaErr := bt.saveMeta()
	if metaErr != nil {
		fmt.Fprintf(os.Stderr, "关闭时保存元数据出错: %v\n", metaErr)
		// 即使保存元数据失败，也要继续尝试关闭 Pager
	}

	// 2. 关闭 Pager (它会负责刷新所有脏页和同步文件)
	pagerErr := bt.pager.Close()
	if pagerErr != nil {
		fmt.Fprintf(os.Stderr, "关闭 Pager 出错: %v\n", pagerErr)
	}
	// fmt.Println("BTree 已关闭。")

	// 返回遇到的第一个错误
	if metaErr != nil {
		return metaErr
	}
	return pagerErr
}

// --- B+树 搜索操作 ---

// Search 在 B+树中查找给定键关联的值。
// 返回找到的值的副本，或 ErrKeyNotFound。
func (bt *BTree) Search(key []byte) ([]byte, error) {
	bt.mu.RLock() // 加读锁获取根节点 ID
	rootID := bt.rootPageID
	bt.mu.RUnlock()

	if rootID == 0 { // 应该只在未初始化时发生，但以防万一
		return nil, errors.New("搜索错误：B+树根节点 ID 无效 (0)")
	}

	// 获取根节点
	rootNode, err := bt.getNode(rootID)
	if err != nil {
		return nil, fmt.Errorf("搜索时获取根节点 %d 失败: %w", rootID, err)
	}

	// 从根节点开始递归搜索，最终目标是叶子节点
	leafNode, index, err := bt.findLeaf(rootNode, key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) { // findLeaf 可能返回 KeyNotFound
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("搜索时查找叶子节点失败: %w", err)
	}

	// 在找到的叶子节点中检查键是否存在
	if index < int(leafNode.numKeys) && bytes.Equal(leafNode.items[index].Key, key) {
		// 找到了键，返回值的副本
		valueCopy := make([]byte, len(leafNode.items[index].Value))
		copy(valueCopy, leafNode.items[index].Value)
		return valueCopy, nil
	}

	// 如果 findLeaf 返回了叶子节点，但键不在预期位置，说明未找到
	return nil, ErrKeyNotFound
}

// findLeaf 是一个辅助函数，用于从指定节点开始查找包含目标键（或应该包含目标键）的叶子节点。
// 返回找到的叶子节点、键在叶子节点中的索引（如果找到精确匹配）或应该插入的位置，以及可能的错误。
func (bt *BTree) findLeaf(node *Node, key []byte) (*Node, int, error) {
	currentNode := node

	for !currentNode.isLeaf {
		// 在内部节点中查找第一个 >= key 的键索引
		i := sort.Search(int(currentNode.numKeys), func(i int) bool {
			return bytes.Compare(currentNode.items[i].Key, key) >= 0
		})

		// 决定下降到哪个子节点
		var childID PageID
		if i < int(currentNode.numKeys) && bytes.Equal(currentNode.items[i].Key, key) {
			// 如果在内部节点找到完全匹配的键，B+树规则是继续向 *右* 子节点搜索
			// 因为实际数据总是在叶子节点，并且内部节点的键是其右子树的最小值
			childID = currentNode.children[i+1]
		} else {
			// 如果 key 小于 node.items[i].Key，或者 key 大于所有键，
			// 则下降到索引 i 对应的子节点
			childID = currentNode.children[i]
		}

		if childID == 0 {
			// 防御性检查，理论上不应发生
			return nil, 0, fmt.Errorf("内部错误：在内部节点 %d 发现零子页面ID (索引 %d)", currentNode.pageID, i)
		}

		// 获取子节点
		nextNode, err := bt.getNode(childID)
		if err != nil {
			return nil, 0, fmt.Errorf("获取子节点 %d (来自父节点 %d) 失败: %w", childID, currentNode.pageID, err)
		}
		currentNode = nextNode // 更新当前节点，继续循环
	}

	// 到达叶子节点 (currentNode 现在是叶子节点)
	// 在叶子节点中查找键的位置
	i := sort.Search(int(currentNode.numKeys), func(i int) bool {
		return bytes.Compare(currentNode.items[i].Key, key) >= 0
	})

	// 返回叶子节点和键的索引（或插入位置）
	return currentNode, i, nil
}

// --- B+树 插入操作 ---

// Insert 将新的键值对插入到 B+树中。
// 如果键已存在，则返回 ErrKeyExists。
func (bt *BTree) Insert(key []byte, value []byte) error {
	bt.mu.Lock() // 加写锁，因为插入可能修改树结构，包括根节点

	rootID := bt.rootPageID
	if rootID == 0 { // 防御性检查
		bt.mu.Unlock()
		return errors.New("插入错误：无效的根节点 ID (0)")
	}

	rootNode, err := bt.getNode(rootID)
	if err != nil {
		bt.mu.Unlock()
		return fmt.Errorf("插入时获取根节点 %d 失败: %w", rootID, err)
	}

	// --- 处理根节点分裂 ---
	// 如果根节点已满，必须在插入前分裂它。这是 B+树向上增长的方式。
	if rootNode.isFull() {
		// fmt.Printf("根节点 %d 已满，正在分裂...\n", rootID)
		// 分裂根节点，这会创建一个新的根节点
		newRootNode, err := bt.splitRoot(rootNode)
		if err != nil {
			bt.mu.Unlock()
			return fmt.Errorf("分裂根节点 %d 失败: %w", rootID, err)
		}

		// 更新 BTree 的根节点 ID，并标记元数据为脏
		bt.rootPageID = newRootNode.pageID
		bt.metaDirty = true
		// fmt.Printf("根分裂完成。新根节点是 %d。\n", bt.rootPageID)

		// 在根分裂后立即保存元数据是一种安全策略，确保即使后续插入失败，根指针也是正确的。
		// 但也可能影响性能。可以选择只在事务提交或 Close 时保存。这里选择立即保存。
		if metaErr := bt.saveMetaInternal(); metaErr != nil {
			// 这是一个严重问题：树结构已改变，但无法持久化新的根指针。
			bt.mu.Unlock()
			// 数据库状态可能不一致！
			return fmt.Errorf("严重错误：根分裂后保存元数据失败 (新根 %d): %w", bt.rootPageID, metaErr)
		}
		// 更新 rootNode 指向新的根，以便后续 insertNonFull 从新根开始
		rootNode = newRootNode
	}

	// 根节点处理完毕 (可能已分裂并更新)，现在可以安全地解锁 BTree 级别的锁
	// 接下来的操作将只影响 rootNode 或其子孙，节点级别的修改由 putNode 处理
	bt.mu.Unlock()

	// --- 向非满节点插入 ---
	// 从（可能更新后的）根节点开始，递归插入到保证非满的节点中
	err = bt.insertNonFull(rootNode, key, value)
	if err != nil {
		return fmt.Errorf("插入键 '%s' 失败: %w", string(key), err)
	}

	// 可选：插入后是否触发刷新？通常批量操作后或事务结束时刷新。
	// bt.pager.FlushDirtyPages()

	return nil // 插入成功
}

// splitRoot 分裂已满的根节点。
// 返回新的根节点对象。调用者需要更新 BTree 的 rootPageID。
// (调用时需持有 BTree 写锁)
func (bt *BTree) splitRoot(oldRoot *Node) (*Node, error) {
	// 1. 为新的兄弟节点分配页面 (旧根将被分裂成两个子节点)
	siblingID, err := bt.pager.AllocatePage()
	if err != nil {
		return nil, fmt.Errorf("为根分裂的兄弟节点分配页面失败: %w", err)
	}

	// 2. 创建新的兄弟节点
	siblingNode := &Node{
		pageID:  siblingID,
		isLeaf:  oldRoot.isLeaf, // 兄弟节点与旧根具有相同的叶子状态
		numKeys: 0,              // 将从旧根分裂得到 t-1 个键
		// 预分配空间，注意旧根可能是叶子或内部节点
		items:    make([]Item, bt.degree-1), // 最多 t-1 个键
		children: nil,                       // 如果是内部节点，需要分配
		nextLeaf: 0,                         // 如果是叶子节点，需要设置
		pager:    bt.pager,
		btree:    bt,
		dirty:    true, // 新节点
	}

	// 3. 计算分裂点 (中间键的索引)
	middleIndex := bt.degree - 1 // t-1

	// 4. 将旧根的后半部分键/项移动到新兄弟节点
	//    键从 middleIndex+1 到末尾 -> 兄弟节点
	copy(siblingNode.items, oldRoot.items[middleIndex+1:])
	siblingNode.numKeys = uint16(len(oldRoot.items) - (middleIndex + 1)) // t-1 个键

	// 5. 如果旧根是内部节点，移动后半部分的子节点指针
	if !oldRoot.isLeaf {
		// 子节点从 middleIndex+1 (即第 t 个子节点) 到末尾 -> 兄弟节点
		numChildrenToMove := bt.degree // t 个子节点
		siblingNode.children = make([]PageID, numChildrenToMove)
		copy(siblingNode.children, oldRoot.children[middleIndex+1:])
	}

	// 6. 如果旧根是叶子节点，设置兄弟节点的 nextLeaf 指针，并更新旧根的 nextLeaf
	if oldRoot.isLeaf {
		siblingNode.nextLeaf = oldRoot.nextLeaf // 兄弟指向旧根原来的下一个
		oldRoot.nextLeaf = siblingID            // 旧根指向新兄弟
	}

	// 7. 获取要提升到新根的中间键 (仅键部分)
	promotedKey := make([]byte, len(oldRoot.items[middleIndex].Key))
	copy(promotedKey, oldRoot.items[middleIndex].Key)

	// 8. 清理并缩减旧根节点
	//    保留前 t-1 个键
	oldRoot.numKeys = uint16(middleIndex)
	// 清理被移动的项（有助于 GC 和调试）
	// for k := oldRoot.numKeys; k < uint16(len(oldRoot.items)); k++ {
	//	 oldRoot.items[k] = Item{}
	// }
	oldRoot.items = oldRoot.items[:oldRoot.numKeys] // 调整切片大小
	if !oldRoot.isLeaf {
		// 保留前 t 个子节点
		oldRoot.children = oldRoot.children[:middleIndex+1]
	}
	oldRoot.dirty = true // 旧根已被修改

	// 9. 为新的根节点分配页面
	newRootID, err := bt.pager.AllocatePage()
	if err != nil {
		// 清理已分配的 siblingID？复杂的回滚。
		return nil, fmt.Errorf("为新根节点分配页面失败: %w", err)
	}

	// 10. 创建新的根节点 (它将是内部节点)
	newRoot := &Node{
		pageID:   newRootID,
		isLeaf:   false,                               // 新根总是内部节点
		numKeys:  1,                                   // 只有一个提升上来的键
		items:    []Item{{Key: promotedKey}},          // 包含提升的键
		children: []PageID{oldRoot.pageID, siblingID}, // 指向旧根和新兄弟
		pager:    bt.pager,
		btree:    bt,
		dirty:    true, // 新节点
	}

	// 11. 将所有修改过的节点写回 Pager
	errOld := bt.putNode(oldRoot)
	errSibling := bt.putNode(siblingNode)
	errNewRoot := bt.putNode(newRoot)

	// 检查写入错误
	if errOld != nil {
		return nil, fmt.Errorf("分裂根时写入旧根 %d 失败: %w. %w", oldRoot.pageID, errOld, ErrNodeSplitFailed)
	}
	if errSibling != nil {
		return nil, fmt.Errorf("分裂根时写入新兄弟 %d 失败: %w. %w", siblingID, errSibling, ErrNodeSplitFailed)
	}
	if errNewRoot != nil {
		return nil, fmt.Errorf("分裂根时写入新根 %d 失败: %w. %w", newRootID, errNewRoot, ErrNodeSplitFailed)
	}

	// 返回新的根节点对象
	return newRoot, nil
}

// insertNonFull 将键值对插入到一个保证非满的节点中。
// 这是插入操作的核心递归函数。
func (bt *BTree) insertNonFull(node *Node, key []byte, value []byte) error {
	// --- 情况 1: 当前节点是叶子节点 ---
	if node.isLeaf {
		// 找到键应该插入的位置
		i := sort.Search(int(node.numKeys), func(idx int) bool {
			return bytes.Compare(node.items[idx].Key, key) >= 0
		})

		// 检查键是否已存在
		if i < int(node.numKeys) && bytes.Equal(node.items[i].Key, key) {
			return fmt.Errorf("在叶子节点 %d 插入失败: %w: '%s'", node.pageID, ErrKeyExists, string(key))
		}

		// 键不存在，执行插入
		// 创建新项的副本
		newItem := Item{Key: make([]byte, len(key)), Value: make([]byte, len(value))}
		copy(newItem.Key, key)
		copy(newItem.Value, value)

		// 为新项腾出空间
		// 如果 node.items 的容量不足，append 会重新分配，否则会原地修改
		node.items = append(node.items, Item{}) // 追加一个空项以扩展（或触发重分配）
		copy(node.items[i+1:], node.items[i:])  // 将 i 及之后的项向右移动一个位置
		node.items[i] = newItem                 // 在位置 i 插入新项
		node.numKeys++                          // 增加键计数
		node.dirty = true                       // 标记节点已修改

		// 将修改后的叶子节点写回 Pager
		err := bt.putNode(node)
		if err != nil {
			// 内存状态与磁盘可能不一致，这是个问题
			// 尝试回滚内存中的修改？复杂。
			return fmt.Errorf("插入后写入叶子节点 %d 失败: %w", node.pageID, err)
		}
		return nil // 叶子节点插入成功
	}

	// --- 情况 2: 当前节点是内部节点 ---
	// 找到应该下降的子节点索引
	i := sort.Search(int(node.numKeys), func(idx int) bool {
		return bytes.Compare(node.items[idx].Key, key) >= 0
	})

	// B+树中，即使内部节点的键与插入键相同，我们也需要继续下降，
	// 因为实际数据总是在叶子节点。
	// 如果 key >= node.items[i].Key (即 sort.Search 找到的位置 i)，并且键匹配，
	// 或者 key < node.items[i].Key，我们都应该下降到 children[i]。
	// 如果 key > 所有 node.items 中的键，i 将是 numKeys，我们下降到 children[numKeys]。
	// 所以，要下降的子节点索引总是 'i'。

	childIndex := i
	childID := node.children[childIndex]

	// 获取子节点
	childNode, err := bt.getNode(childID)
	if err != nil {
		return fmt.Errorf("插入时获取子节点 %d (索引 %d) 失败: %w", childID, childIndex, err)
	}

	// --- 检查子节点是否需要分裂 ---
	// B+树插入策略：在下降到子节点 *之前*，如果子节点已满，则先分裂子节点。
	// 这保证了递归路径上的所有节点都不会满，简化了插入逻辑。
	if childNode.isFull() {
		// fmt.Printf("子节点 %d (父节点 %d, 索引 %d) 已满，正在分裂...\n", childID, node.pageID, childIndex)
		// 分裂子节点。这会修改父节点 'node'（提升键、增加子节点指针）。
		err = bt.splitChild(node, childIndex, childNode)
		if err != nil {
			return fmt.Errorf("分裂子节点 %d (索引 %d) 失败: %w", childID, childIndex, err)
		}
		// fmt.Printf("子节点分裂完成。父节点 %d 已更新。\n", node.pageID)

		// 分裂后，父节点 'node' 有了一个新的键 (在索引 childIndex) 和一个新的子节点 (在索引 childIndex+1)。
		// 我们需要决定现在应该下降到哪个子节点：
		// - 如果插入的键 `key` 小于新提升到父节点的键 `node.items[childIndex].Key`，
		//   那么 `key` 仍然属于原来的子节点（现在是 `node.children[childIndex]`）。
		// - 如果插入的键 `key` 大于或等于新提升的键，
		//   那么 `key` 应该插入到新的兄弟子节点（现在是 `node.children[childIndex+1]`）。

		if bytes.Compare(key, node.items[childIndex].Key) >= 0 {
			// 需要下降到新的兄弟节点
			childIndex++ // 更新子节点索引
			childID = node.children[childIndex]
			// 重新获取分裂后可能已更改的节点引用
			childNode, err = bt.getNode(childID)
			if err != nil {
				return fmt.Errorf("分裂后获取新兄弟节点 %d (索引 %d) 失败: %w", childID, childIndex, err)
			}
		} else {
			// 仍然下降到原来的子节点，但它可能已被分裂操作修改（键数减少）
			// 重新获取节点引用以确保状态最新
			childNode, err = bt.getNode(childID) // childID 未变
			if err != nil {
				return fmt.Errorf("分裂后重新获取子节点 %d (索引 %d) 失败: %w", childID, childIndex, err)
			}
		}
		// 此时的 childNode 保证不是满的。
	}

	// --- 递归插入 ---
	// 现在，目标子节点 (childNode) 保证不是满的，递归调用 insertNonFull。
	return bt.insertNonFull(childNode, key, value)
}

// splitChild 分裂一个非满父节点的指定已满子节点。
// `parent`: 非满的父节点。
// `childIndex`: 已满子节点在父节点 children 数组中的索引。
// `child`: 已满的子节点对象。
// (调用此函数时，不需要持有 BTree 的全局锁，但需要确保对 parent, child 的并发访问是安全的，
//
//	通常通过调用者 insertNonFull 的逻辑保证，或者需要节点级锁)
func (bt *BTree) splitChild(parent *Node, childIndex int, child *Node) error {
	// 基本检查
	if parent.isLeaf {
		return errors.New("内部错误：不能对叶子节点的子节点调用 splitChild")
	}
	if !child.isFull() {
		// 理论上调用者应保证 child 是满的
		return errors.New("内部错误：尝试分裂非满子节点")
	}
	if parent.isFull() {
		// 理论上调用者应保证 parent 是非满的
		return errors.New("内部错误：尝试在父节点已满时分裂子节点")
	}

	// 1. 为新兄弟节点分配页面
	siblingID, err := bt.pager.AllocatePage()
	if err != nil {
		return fmt.Errorf("分裂子节点 %d 时为兄弟节点分配页面失败: %w", child.pageID, err)
	}

	// 2. 创建新的兄弟节点
	sibling := &Node{
		pageID:   siblingID,
		isLeaf:   child.isLeaf,              // 与被分裂的子节点类型相同
		numKeys:  uint16(bt.degree - 1),     // 新兄弟获得 t-1 个键
		items:    make([]Item, bt.degree-1), // 分配空间
		children: nil,                       // 如果是内部节点，稍后分配
		nextLeaf: 0,                         // 如果是叶子节点，稍后设置
		pager:    bt.pager,
		btree:    bt,
		dirty:    true, // 新节点
	}

	// 3. 计算分裂点
	middleIndex := bt.degree - 1 // 中间键的索引 (t-1)

	// 4. 将 `child` 的后半部分键/项移动到 `sibling`
	copy(sibling.items, child.items[middleIndex+1:])

	// 5. 如果是内部节点，移动后半部分的子节点指针
	if !child.isLeaf {
		numChildrenToMove := bt.degree // t 个子节点
		sibling.children = make([]PageID, numChildrenToMove)
		copy(sibling.children, child.children[middleIndex+1:])
	}

	// 6. 如果是叶子节点
	if child.isLeaf {
		sibling.nextLeaf = child.nextLeaf // 新兄弟指向 child 原来的下一个
		child.nextLeaf = sibling.pageID   // child 指向新兄弟
		sibling.prevLeaf = child.pageID   // 新兄弟的前一个是 child
		child.prevLeaf = 0                // 如果 child 是第一个节点，prevLeaf 保持为 0

		// 更新原 nextLeaf 的 prevLeaf 指针
		if sibling.nextLeaf != 0 {
			nextNode, err := bt.getNode(sibling.nextLeaf)
			if err != nil {
				return fmt.Errorf("分裂时获取下一个叶子节点 %d 失败: %w", sibling.nextLeaf, err)
			}
			nextNode.prevLeaf = sibling.pageID
			nextNode.dirty = true
			if err := bt.putNode(nextNode); err != nil {
				return fmt.Errorf("分裂时更新下一个叶子节点 %d 失败: %w", sibling.nextLeaf, err)
			}
		}
	}

	// 7. 获取要提升到父节点的中间键（仅键）
	promotedKey := make([]byte, len(child.items[middleIndex].Key))
	copy(promotedKey, child.items[middleIndex].Key)

	// 8. 缩减原始 `child` 节点
	//   保留前 t-1 个键
	child.numKeys = uint16(middleIndex)
	//   调整 items 切片大小
	//   需要注意切片的底层数组是否共享，如果直接缩容可能影响 sibling 的数据
	//   安全的做法是创建一个新的切片，或确保 copy 是深拷贝
	child.items = child.items[:child.numKeys] // 截断
	if !child.isLeaf {
		// 保留前 t 个子节点
		child.children = child.children[:middleIndex+1] // 截断
	}
	child.dirty = true // child 已被修改

	// 9. 将新兄弟节点的页面ID插入到父节点的 children 数组中
	//    在 childIndex+1 位置腾出空间
	parent.children = append(parent.children, 0)                         // 扩展容量
	copy(parent.children[childIndex+2:], parent.children[childIndex+1:]) // 右移
	parent.children[childIndex+1] = siblingID                            // 插入 sibling ID

	// 10. 将提升的键插入到父节点的 items 数组中
	//     在 childIndex 位置腾出空间
	promotedItem := Item{Key: promotedKey}                       // 父节点只关心键
	parent.items = append(parent.items, Item{})                  // 扩展容量
	copy(parent.items[childIndex+1:], parent.items[childIndex:]) // 右移
	parent.items[childIndex] = promotedItem                      // 插入提升的键
	parent.numKeys++                                             // 增加父节点键数
	parent.dirty = true                                          // 父节点已被修改

	// 11. 将所有修改过的节点写回 Pager
	errChild := bt.putNode(child)
	errSibling := bt.putNode(sibling)
	errParent := bt.putNode(parent) // 父节点必须在子节点之后写吗？取决于 Pager 实现和崩溃恢复策略。通常可以。

	// 检查写入错误
	if errChild != nil {
		return fmt.Errorf("splitChild 写入原始子节点 %d 失败: %w. %w", child.pageID, errChild, ErrNodeSplitFailed)
	}
	if errSibling != nil {
		return fmt.Errorf("splitChild 写入新兄弟节点 %d 失败: %w. %w", sibling.pageID, errSibling, ErrNodeSplitFailed)
	}
	if errParent != nil {
		return fmt.Errorf("splitChild 写入父节点 %d 失败: %w. %w", parent.pageID, errParent, ErrNodeSplitFailed)
	}

	return nil // 分裂成功
}

// mergeLeaves 合并两个相邻的叶子节点。
// 参数说明：
// - left: 左叶子节点（当前节点）。
// - right: 右叶子节点（兄弟节点）。
// - parent: 父节点。
// - keyIndexInParent: 左叶子节点在父节点中的键索引。
func (bt *BTree) mergeLeaves(left *Node, right *Node, parent *Node, keyIndexInParent int) error {
	// 基本检查
	if !left.isLeaf || !right.isLeaf {
		return errors.New("内部错误：mergeLeaves 只能用于叶子节点")
	}
	if int(left.numKeys)+int(right.numKeys) > left.maxKeys() {
		return errors.New("内部错误：合并后的叶子节点会超出最大键数限制")
	}

	// 1. 将右叶子节点的所有项移动到左叶子节点
	left.items = append(left.items, right.items...)
	left.numKeys += right.numKeys

	// 2. 更新左叶子节点的 nextLeaf 指针
	left.nextLeaf = right.nextLeaf
	if right.nextLeaf != 0 {
		// 更新右兄弟节点的 prevLeaf 指针
		nextNode, err := bt.getNode(right.nextLeaf)
		if err != nil {
			return fmt.Errorf("合并时获取右兄弟节点 %d 失败: %w", right.nextLeaf, err)
		}
		nextNode.prevLeaf = left.pageID
		nextNode.dirty = true
		if err := bt.putNode(nextNode); err != nil {
			return fmt.Errorf("合并时更新右兄弟节点 %d 失败: %w", right.nextLeaf, err)
		}
	}

	// 3. 从父节点中移除指向右叶子节点的键和指针
	parent.numKeys--
	parent.items = append(parent.items[:keyIndexInParent], parent.items[keyIndexInParent+1:]...)
	parent.children = append(parent.children[:keyIndexInParent+1], parent.children[keyIndexInParent+2:]...)
	parent.dirty = true

	// 4. 标记右叶子节点为无效（可选：释放页面）
	right.numKeys = 0
	right.items = nil
	right.nextLeaf = 0
	right.prevLeaf = 0
	right.dirty = true

	// 5. 将所有修改写回 Pager
	errLeft := bt.putNode(left)
	errParent := bt.putNode(parent)
	errRight := bt.putNode(right)

	// 检查写入错误
	if errLeft != nil {
		return fmt.Errorf("合并叶子节点时写入左节点 %d 失败: %w", left.pageID, errLeft)
	}
	if errParent != nil {
		return fmt.Errorf("合并叶子节点时写入父节点 %d 失败: %w", parent.pageID, errParent)
	}
	if errRight != nil {
		return fmt.Errorf("合并叶子节点时写入右节点 %d 失败: %w", right.pageID, errRight)
	}

	return nil
}

// findLeftmostLeaf 从指定的节点开始递归查找最左侧的叶子节点。
func (bt *BTree) findLeftmostLeaf(node *Node) (*Node, error) {
	currentNode := node

	// 循环向下遍历，直到找到叶子节点
	for !currentNode.isLeaf {
		// 内部节点的第一个子节点是最左侧路径
		if len(currentNode.children) == 0 {
			return nil, fmt.Errorf("内部错误：节点 %d 没有子节点", currentNode.pageID)
		}
		childID := currentNode.children[0]

		// 获取子节点
		nextNode, err := bt.getNode(childID)
		if err != nil {
			return nil, fmt.Errorf("获取子节点 %d (来自父节点 %d) 失败: %w", childID, currentNode.pageID, err)
		}
		currentNode = nextNode
	}

	// 返回找到的最左侧叶子节点
	return currentNode, nil
}

// validateLeafLinks 验证 B+ 树中所有叶子节点的双向链表是否正确连接。
func (bt *BTree) validateLeafLinks() error {
	bt.mu.RLock() // 加读锁以保护对树结构的访问
	defer bt.mu.RUnlock()

	rootID := bt.rootPageID
	if rootID == 0 {
		return errors.New("验证错误：无效的根节点 ID (0)")
	}

	// 获取根节点
	rootNode, err := bt.getNode(rootID)
	if err != nil {
		return fmt.Errorf("验证时获取根节点 %d 失败: %w", rootID, err)
	}

	// 找到最左侧的叶子节点
	leftmostLeaf, err := bt.findLeftmostLeaf(rootNode)
	if err != nil {
		return fmt.Errorf("查找最左侧叶子节点失败: %w", err)
	}

	// 遍历叶子链表，验证 prevLeaf 和 nextLeaf 的一致性
	var previousLeaf *Node = nil
	currentLeaf := leftmostLeaf

	for currentLeaf != nil {
		// 检查 prevLeaf 指针
		if currentLeaf.prevLeaf != 0 {
			if previousLeaf == nil || currentLeaf.prevLeaf != previousLeaf.pageID {
				return fmt.Errorf("叶子节点 %d 的 prevLeaf 指针不一致: 期望 %d, 实际 %d",
					currentLeaf.pageID, previousLeaf.pageID, currentLeaf.prevLeaf)
			}
		} else if previousLeaf != nil {
			// 如果 prevLeaf 为 0，但当前节点不是最左侧节点
			return fmt.Errorf("叶子节点 %d 的 prevLeaf 指针为 0，但它不是最左侧节点", currentLeaf.pageID)
		}

		// 更新 previousLeaf
		previousLeaf = currentLeaf

		// 移动到下一个叶子节点
		if currentLeaf.nextLeaf != 0 {
			nextLeaf, err := bt.getNode(currentLeaf.nextLeaf)
			if err != nil {
				return fmt.Errorf("获取下一个叶子节点 %d 失败: %w", currentLeaf.nextLeaf, err)
			}
			currentLeaf = nextLeaf
		} else {
			// 到达链表末尾
			break
		}
	}

	return nil
}

// Delete 从 B+树中删除指定的键。
// 如果键不存在，返回 ErrKeyNotFound。
func (bt *BTree) Delete(key []byte) error {
	bt.mu.Lock() // 加写锁，删除可能修改树结构
	defer bt.mu.Unlock()

	rootID := bt.rootPageID
	if rootID == 0 {
		return errors.New("删除错误：无效的根节点 ID (0)")
	}

	// 调用递归删除辅助函数
	err := bt.deleteInternal(nil, rootID, key) // 从根节点开始，父节点为 nil
	if err != nil {
		return err // 返回遇到的错误，如 ErrKeyNotFound
	}

	// --- 处理根节点可能发生的下溢 ---
	// 如果删除导致根节点不再需要（例如，根节点只有一个子节点且没有键），
	// 则树的高度需要降低。
	rootNode, getErr := bt.getNode(bt.rootPageID)
	if getErr != nil {
		// 严重错误：无法在删除后获取根节点
		return fmt.Errorf("严重错误：删除后获取根节点 %d 失败: %w", bt.rootPageID, getErr)
	}

	// 如果根节点是内部节点，并且键数量变为 0
	if !rootNode.isLeaf && rootNode.numKeys == 0 {
		// 根节点现在只有一个子节点（在 children[0]）
		// 这个子节点成为新的根节点
		oldRootID := bt.rootPageID
		bt.rootPageID = rootNode.children[0]
		bt.metaDirty = true
		// fmt.Printf("树高度降低。旧根 %d 删除，新根 %d。\n", oldRootID, bt.rootPageID)

		// 保存元数据更新
		if metaErr := bt.saveMetaInternal(); metaErr != nil {
			// 无法保存新的根指针，状态可能不一致
			return fmt.Errorf("严重错误：降低树高度后保存元数据失败 (新根 %d): %w", bt.rootPageID, metaErr)
		}

		// TODO: 理想情况下，应该将旧的根页面 ID (oldRootID) 添加到空闲列表以供重用。
		// 在这个实现中，我们暂时只是“遗弃”它。
	}

	// 可选：删除后是否触发刷新？
	// bt.pager.FlushDirtyPages()

	return nil // 删除成功
}

// deleteInternal 是 Delete 的递归辅助函数。
// parent: 当前节点 node 的父节点 (用于处理下溢)。根节点的父节点为 nil。
// nodeID: 当前正在处理的节点的页面 ID。
// key: 要删除的键。
// 返回错误，例如 ErrKeyNotFound 或写入失败。
func (bt *BTree) deleteInternal(parent *Node, nodeID PageID, key []byte) error {
	node, err := bt.getNode(nodeID)
	if err != nil {
		return fmt.Errorf("deleteInternal 获取节点 %d 失败: %w", nodeID, err)
	}

	// --- 1. 处理叶子节点 ---
	if node.isLeaf {
		// 在叶子节点中查找键
		i := sort.Search(int(node.numKeys), func(idx int) bool {
			return bytes.Compare(node.items[idx].Key, key) >= 0
		})

		// 检查键是否存在
		if i >= int(node.numKeys) || !bytes.Equal(node.items[i].Key, key) {
			return ErrKeyNotFound // 键在叶子节点中未找到
		}

		// --- 找到了键，执行删除 ---
		// fmt.Printf("在叶子节点 %d 位置 %d 删除键 '%s'\n", nodeID, i, string(key))
		// 从 items 切片中移除键值对
		copy(node.items[i:], node.items[i+1:])
		// 清理最后一个元素（可选，有助于 GC）
		node.items[len(node.items)-1] = Item{}
		node.items = node.items[:len(node.items)-1]
		node.numKeys--
		node.dirty = true

		// 将修改后的叶子节点写回 Pager
		if err := bt.putNode(node); err != nil {
			return fmt.Errorf("删除后写入叶子节点 %d 失败: %w", nodeID, err)
		}

		// 叶子节点删除后不需要进一步递归，但需要在返回后由调用者检查下溢。
		return nil
	}

	// --- 2. 处理内部节点 ---
	// 找到应该下降的子节点索引 i
	i := sort.Search(int(node.numKeys), func(idx int) bool {
		return bytes.Compare(node.items[idx].Key, key) >= 0
	})
	// 如果找到完全匹配的键 key == node.items[i].Key，B+树规则要求我们删除左子树中小于key的最大值
	// 或右子树中大于等于key的最小值，并用其替换内部节点中的key。
	// 这个实现简化了这一点：我们总是递归到叶子节点删除，并在处理下溢时调整内部节点。
	// 因此，即使 key == node.items[i].Key，我们也下降到左子树 children[i]。
	// 如果 key > node.items[i].Key 或 key > 所有键，下降到 children[i]。
	childIndex := i
	childID := node.children[childIndex]

	// --- 预处理：确保即将访问的子节点不会在删除后下溢 ---
	// 获取子节点信息（不需要完整加载节点，仅检查键数可能更优，但这里简化）
	childNodeForCheck, err := bt.getNode(childID)
	if err != nil {
		return fmt.Errorf("删除时预检查子节点 %d (父 %d) 失败: %w", childID, nodeID, err)
	}

	needsHandling := childNodeForCheck.numKeys == uint16(childNodeForCheck.minKeys()) // 子节点当前处于最小键数

	if needsHandling {
		// fmt.Printf("预处理：子节点 %d (父 %d, 索引 %d) 处于最小键数，尝试处理...\n", childID, nodeID, childIndex)
		// 子节点可能在删除后下溢，需要先处理：借用或合并
		childNodeHandled, err := bt.handlePotentialUnderflow(node, childIndex)
		if err != nil {
			return fmt.Errorf("处理子节点 %d (父 %d) 的潜在下溢失败: %w", childID, nodeID, err)
		}
		// handlePotentialUnderflow 可能会合并节点，导致父节点中的子节点指针变化
		// 更新 childID 和 childIndex 以反映可能的变化
		// 注意：如果发生合并，原来的 childIndex 可能不再有效，或者指向了合并后的节点
		// 重新确定正确的 childIndex 和 childID 进行递归
		newChildIndex := sort.Search(int(node.numKeys), func(idx int) bool {
			return bytes.Compare(node.items[idx].Key, key) >= 0
		})
		childIndex = newChildIndex // 更新 childIndex
		childID = node.children[childIndex]
		// fmt.Printf("下溢处理后，将递归到子节点 %d (父 %d, 新索引 %d)\n", childID, nodeID, childIndex)
		_ = childNodeHandled // 使用 childNodeHandled 保证变量被使用

		// 获取处理后的实际子节点（可能是合并或借用后的节点）
		// childNodeForRecursion, err := bt.getNode(childID)
		// if err != nil {
		//     return fmt.Errorf("获取下溢处理后的子节点 %d (父 %d) 失败: %w", childID, nodeID, err)
		// }
		// 现在可以安全地向 childNodeForRecursion 递归
	}

	// --- 递归删除 ---
	err = bt.deleteInternal(node, childID, key)
	if err != nil {
		return err // 将子树中发生的错误（如 KeyNotFound）传递上去
	}

	// --- 删除后的清理 (仅内部节点) ---
	// 如果子节点下溢导致了合并，当前节点(node)可能需要更新。
	// 注意：我们的预处理步骤应该保证了子节点在递归返回时不会处于下溢状态，
	// 但父节点(node)本身可能因为子节点的合并而失去了一个键和子指针。
	// 因此，我们需要检查当前节点 `node` 是否下溢。
	// 但是，根节点的下溢在顶层 Delete 函数中处理。
	// 对于非根内部节点，其下溢会在其父节点的 *下一次* pre-emptive check 中被处理。
	// 所以，在这里似乎不需要显式的下溢检查。

	// --- 处理内部节点键替换 (如果删除发生在内部节点的键上) ---
	// B+树的一个复杂性是，如果删除的键 `key` 也存在于内部节点中，
	// 删除叶子节点中的 `key` 后，需要找到一个新的分隔符键来替换内部节点中的 `key`。
	// 通常是找到被删除 `key` 的 *后继* 键（即其右子树中的最小键），并用它替换内部节点中的 `key`。
	// 这个实现简化了这一点，依赖于合并/借用操作来间接维护内部节点键的正确性。
	// 在某些情况下，这可能导致内部节点的键与子树的实际分隔不完全精确（例如，键可能等于其右子树的最小值），
	// 但搜索逻辑（总是向右子树寻找等值键）仍然有效。
	// 一个更完整的实现需要显式处理键替换。

	return nil
}

// handlePotentialUnderflow 检查父节点的指定子节点是否需要处理（借用或合并）。
// 如果子节点键数等于 minKeys，则执行借用或合并，确保子节点至少有 minKeys 个键。
// 返回处理后的子节点对象（可能与传入的不同，如果发生合并）。
// node: 父节点
// childIndex: 需要检查的子节点在父节点 children 中的索引
func (bt *BTree) handlePotentialUnderflow(parent *Node, childIndex int) (*Node, error) {
	childID := parent.children[childIndex]
	child, err := bt.getNode(childID)
	if err != nil {
		return nil, fmt.Errorf("获取子节点 %d 失败: %w", childID, err)
	}

	if child.numKeys >= uint16(child.minKeys()) {
		// 子节点键数足够，无需处理
		return child, nil // 返回原始子节点
	}
	// fmt.Printf("节点 %d (父 %d, 索引 %d) 键数 %d < minKeys %d，需要处理下溢\n",
	// 	childID, parent.pageID, childIndex, child.numKeys, bt.minKeys())

	// --- 尝试从左兄弟借用 ---
	if childIndex > 0 { // 存在左兄弟
		leftSiblingID := parent.children[childIndex-1]
		leftSibling, err := bt.getNode(leftSiblingID)
		if err != nil {
			return nil, fmt.Errorf("获取左兄弟 %d 失败: %w", leftSiblingID, err)
		}
		if leftSibling.numKeys > uint16(leftSibling.minKeys()) {
			// 左兄弟有多余的键，可以借用
			// fmt.Printf("从左兄弟 %d 向节点 %d 借用\n", leftSiblingID, childID)
			err = bt.borrowFromLeft(parent, childIndex, child, leftSibling)
			if err != nil {
				return nil, fmt.Errorf("从左兄弟 %d 借用失败: %w", leftSiblingID, err)
			}
			// 借用成功后，child 节点已更新并写回，返回更新后的 child
			updatedChild, getErr := bt.getNode(childID) // 重新获取以确保状态最新
			if getErr != nil {
				return nil, fmt.Errorf("获取借用后的子节点 %d 失败: %w", childID, getErr)
			}
			return updatedChild, nil
		}
	}

	// --- 尝试从右兄弟借用 ---
	if childIndex < int(parent.numKeys) { // 存在右兄弟 (注意索引边界)
		rightSiblingID := parent.children[childIndex+1]
		rightSibling, err := bt.getNode(rightSiblingID)
		if err != nil {
			return nil, fmt.Errorf("获取右兄弟 %d 失败: %w", rightSiblingID, err)
		}
		if rightSibling.numKeys > uint16(rightSibling.minKeys()) {
			// 右兄弟有多余的键，可以借用
			// fmt.Printf("从右兄弟 %d 向节点 %d 借用\n", rightSiblingID, childID)
			err = bt.borrowFromRight(parent, childIndex, child, rightSibling)
			if err != nil {
				return nil, fmt.Errorf("从右兄弟 %d 借用失败: %w", rightSiblingID, err)
			}
			// 借用成功后，child 节点已更新并写回，返回更新后的 child
			updatedChild, getErr := bt.getNode(childID) // 重新获取
			if getErr != nil {
				return nil, fmt.Errorf("获取借用后的子节点 %d 失败: %w", childID, getErr)
			}
			return updatedChild, nil
		}
	}

	// --- 借用失败，执行合并 ---
	// fmt.Printf("借用失败，尝试合并节点 %d (索引 %d)\n", childID, childIndex)
	var mergedNode *Node
	if childIndex > 0 {
		// 优先与左兄弟合并 (将 child 合并到 leftSibling)
		leftSiblingID := parent.children[childIndex-1]
		leftSibling, err := bt.getNode(leftSiblingID)
		if err != nil {
			return nil, fmt.Errorf("合并时获取左兄弟 %d 失败: %w", leftSiblingID, err)
		}
		// fmt.Printf("与左兄弟 %d 合并\n", leftSiblingID)
		err = bt.mergeNodes(parent, childIndex-1, leftSibling, child) // merge child into leftSibling
		if err != nil {
			return nil, fmt.Errorf("与左兄弟 %d 合并失败: %w", leftSiblingID, err)
		}
		// 合并后，原来的 child 已被并入 leftSibling，递归应在 leftSibling 上进行
		mergedNode, err = bt.getNode(leftSiblingID) // 获取合并后的节点
		if err != nil {
			return nil, fmt.Errorf("获取与左兄弟合并后的节点 %d 失败: %w", leftSiblingID, err)
		}

	} else {
		// 没有左兄弟，与右兄弟合并 (将 rightSibling 合并到 child)
		if childIndex >= int(parent.numKeys) {
			// 如果childIndex已经是最后一个指针，理论上它必须有右兄弟，除非它是根的唯一子节点
			// 但这种情况应该在根处理逻辑中覆盖。如果在这里发生，说明有问题。
			return nil, fmt.Errorf("内部错误：尝试合并最后一个子节点 %d 时没有右兄弟 (父 %d, numKeys %d)", childID, parent.pageID, parent.numKeys)
		}
		rightSiblingID := parent.children[childIndex+1]
		rightSibling, err := bt.getNode(rightSiblingID)
		if err != nil {
			return nil, fmt.Errorf("合并时获取右兄弟 %d 失败: %w", rightSiblingID, err)
		}
		// fmt.Printf("与右兄弟 %d 合并\n", rightSiblingID)
		err = bt.mergeNodes(parent, childIndex, child, rightSibling) // merge rightSibling into child
		if err != nil {
			return nil, fmt.Errorf("与右兄弟 %d 合并失败: %w", rightSiblingID, err)
		}
		// 合并后，递归仍在 child 上进行，但 child 内容已更新
		mergedNode, err = bt.getNode(childID) // 获取合并后的节点
		if err != nil {
			return nil, fmt.Errorf("获取与右兄弟合并后的节点 %d 失败: %w", childID, err)
		}
	}

	// 返回合并后应该继续递归的节点
	return mergedNode, nil
}

// borrowFromLeft 从左兄弟节点借用一个元素给当前子节点。
// parent: 父节点
// childIndex: 当前子节点在父节点 children 中的索引
// child: 当前子节点 (接收方)
// leftSibling: 左兄弟节点 (提供方)
func (bt *BTree) borrowFromLeft(parent *Node, childIndex int, child *Node, leftSibling *Node) error {
	// 1. 将父节点中分隔左右兄弟的键 `parent.items[childIndex-1]` 下移到 `child` 的开头。
	separatorKeyItem := parent.items[childIndex-1] // 父节点只存 key

	// 为新项腾出空间 (在 child 开头)
	child.items = append(child.items, Item{})          // 扩展
	copy(child.items[1:], child.items[:child.numKeys]) // 右移现有项

	// 如果 child 是叶子，需要从 leftSibling 获取 Key 和 Value
	// 如果 child 是内部节点，只需要 Key，Value 为 nil
	if child.isLeaf {
		// 从 leftSibling 获取最后一项 (Key 和 Value)
		borrowedItem := leftSibling.items[leftSibling.numKeys-1]
		child.items[0] = borrowedItem // 插入到 child 开头
		// 更新父节点的 separator 为 leftSibling 新的最后一项的 Key
		parent.items[childIndex-1] = Item{Key: make([]byte, len(leftSibling.items[leftSibling.numKeys-2].Key))}
		copy(parent.items[childIndex-1].Key, leftSibling.items[leftSibling.numKeys-2].Key)

	} else { // child 是内部节点
		// 将父节点的 separator key 插入 child 开头
		child.items[0] = Item{Key: make([]byte, len(separatorKeyItem.Key))}
		copy(child.items[0].Key, separatorKeyItem.Key) // 只复制 Key

		// 将 leftSibling 的最后一个子指针移动到 child 的开头
		borrowedChildID := leftSibling.children[leftSibling.numKeys]     // 注意是 numKeys 索引
		child.children = append(child.children, 0)                       // 扩展
		copy(child.children[1:], child.children[:len(child.children)-1]) // 右移
		child.children[0] = borrowedChildID

		// 更新父节点的 separator key 为 leftSibling 新的最后一个 key
		parent.items[childIndex-1] = Item{Key: make([]byte, len(leftSibling.items[leftSibling.numKeys-1].Key))}
		copy(parent.items[childIndex-1].Key, leftSibling.items[leftSibling.numKeys-1].Key)

		// 从 leftSibling 移除最后一个子指针
		leftSibling.children = leftSibling.children[:leftSibling.numKeys]
	}

	// 更新 child 的键数
	child.numKeys++

	// 从 leftSibling 移除最后一项 (键或键值)
	leftSibling.numKeys--
	leftSibling.items = leftSibling.items[:leftSibling.numKeys]

	// 标记所有修改过的节点为 dirty
	parent.dirty = true
	child.dirty = true
	leftSibling.dirty = true

	// 将修改写回 Pager
	if err := bt.putNode(parent); err != nil {
		return fmt.Errorf("borrowLeft 写入父节点 %d 失败: %w", parent.pageID, err)
	}
	if err := bt.putNode(child); err != nil {
		return fmt.Errorf("borrowLeft 写入子节点 %d 失败: %w", child.pageID, err)
	}
	if err := bt.putNode(leftSibling); err != nil {
		return fmt.Errorf("borrowLeft 写入左兄弟 %d 失败: %w", leftSibling.pageID, err)
	}

	return nil
}

// borrowFromRight 从右兄弟节点借用一个元素给当前子节点。
// parent: 父节点
// childIndex: 当前子节点在父节点 children 中的索引
// child: 当前子节点 (接收方)
// rightSibling: 右兄弟节点 (提供方)
func (bt *BTree) borrowFromRight(parent *Node, childIndex int, child *Node, rightSibling *Node) error {
	// 1. 将父节点中分隔左右兄弟的键 `parent.items[childIndex]` 下移到 `child` 的末尾。
	separatorKeyItem := parent.items[childIndex]

	if child.isLeaf {
		// 从 rightSibling 获取第一项 (Key 和 Value)
		borrowedItem := rightSibling.items[0]
		// 将 borrowedItem 追加到 child 末尾
		child.items = append(child.items, borrowedItem)
		// 更新父节点的 separator 为 rightSibling 新的第一项的 Key
		parent.items[childIndex] = Item{Key: make([]byte, len(rightSibling.items[1].Key))}
		copy(parent.items[childIndex].Key, rightSibling.items[1].Key)

	} else { // child 是内部节点
		// 将父节点的 separator key 追加到 child 末尾
		newItem := Item{Key: make([]byte, len(separatorKeyItem.Key))}
		copy(newItem.Key, separatorKeyItem.Key)
		child.items = append(child.items, newItem)

		// 将 rightSibling 的第一个子指针移动到 child 的末尾
		borrowedChildID := rightSibling.children[0]
		child.children = append(child.children, borrowedChildID)

		// 更新父节点的 separator key 为 rightSibling 的第一个 key
		parent.items[childIndex] = Item{Key: make([]byte, len(rightSibling.items[0].Key))}
		copy(parent.items[childIndex].Key, rightSibling.items[0].Key)

		// 从 rightSibling 移除第一个子指针
		rightSibling.children = rightSibling.children[1:]
	}

	// 更新 child 的键数
	child.numKeys++

	// 从 rightSibling 移除第一项 (键或键值)
	rightSibling.numKeys--
	copy(rightSibling.items[0:], rightSibling.items[1:])
	rightSibling.items = rightSibling.items[:rightSibling.numKeys]

	// 标记所有修改过的节点为 dirty
	parent.dirty = true
	child.dirty = true
	rightSibling.dirty = true

	// 将修改写回 Pager
	if err := bt.putNode(parent); err != nil {
		return fmt.Errorf("borrowRight 写入父节点 %d 失败: %w", parent.pageID, err)
	}
	if err := bt.putNode(child); err != nil {
		return fmt.Errorf("borrowRight 写入子节点 %d 失败: %w", child.pageID, err)
	}
	if err := bt.putNode(rightSibling); err != nil {
		return fmt.Errorf("borrowRight 写入右兄弟 %d 失败: %w", rightSibling.pageID, err)
	}

	return nil
}

// mergeNodes 合并两个相邻的子节点 (leftChild 和 rightChild)。
// 父节点中的分隔键会下移，rightChild 的内容会移动到 leftChild 中。
// parent: 父节点
// leftChildIndex: 左子节点在父节点 children 中的索引
// leftChild: 左子节点 (合并的目标)
// rightChild: 右子节点 (被合并的源)
func (bt *BTree) mergeNodes(parent *Node, leftChildIndex int, leftChild *Node, rightChild *Node) error {
	// fmt.Printf("合并节点 %d (右) 到节点 %d (左), 父 %d, 分隔键索引 %d\n",
	// 	rightChild.pageID, leftChild.pageID, parent.pageID, leftChildIndex)

	if leftChild.isLeaf != rightChild.isLeaf {
		return errors.New("内部错误：尝试合并叶子节点和内部节点")
	}

	// 1. 如果是内部节点，需要将父节点的分隔键下移到 leftChild 的末尾。
	//    如果是叶子节点，分隔键不需要下移，因为叶子节点存储完整的键值对。
	if !leftChild.isLeaf {
		separatorKeyItem := parent.items[leftChildIndex]
		newKey := Item{Key: make([]byte, len(separatorKeyItem.Key))}
		copy(newKey.Key, separatorKeyItem.Key)
		leftChild.items = append(leftChild.items, newKey)
		leftChild.numKeys++ // 增加因分隔键下移而来的键
	}

	// 2. 将 rightChild 的所有 items 移动到 leftChild 的末尾。
	leftChild.items = append(leftChild.items, rightChild.items...)
	leftChild.numKeys += rightChild.numKeys

	// 3. 如果是内部节点，将 rightChild 的所有 children 指针移动到 leftChild 的末尾。
	if !leftChild.isLeaf {
		leftChild.children = append(leftChild.children, rightChild.children...)
	}

	// 4. 如果是叶子节点，更新链表指针。
	if leftChild.isLeaf {
		leftChild.nextLeaf = rightChild.nextLeaf
		if rightChild.nextLeaf != 0 {
			// 更新下一节点的 prev 指针
			nextNode, err := bt.getNode(rightChild.nextLeaf)
			if err != nil {
				return fmt.Errorf("合并时获取 %d 的下一叶子节点 %d 失败: %w", rightChild.pageID, rightChild.nextLeaf, err)
			}
			nextNode.prevLeaf = leftChild.pageID
			nextNode.dirty = true
			if err := bt.putNode(nextNode); err != nil {
				return fmt.Errorf("合并时更新下一叶子节点 %d 的 prev 指针失败: %w", nextNode.pageID, err)
			}
		}
	}

	// 5. 从父节点中移除分隔键和指向 rightChild 的指针。
	// 移除分隔键 parent.items[leftChildIndex]
	copy(parent.items[leftChildIndex:], parent.items[leftChildIndex+1:])
	parent.items = parent.items[:parent.numKeys-1]

	// 移除子指针 parent.children[leftChildIndex+1] (指向 rightChild)
	copy(parent.children[leftChildIndex+1:], parent.children[leftChildIndex+2:])
	parent.children = parent.children[:parent.numKeys] // 指针数比键数多1，所以是 numKeys

	parent.numKeys--

	// 标记修改
	parent.dirty = true
	leftChild.dirty = true
	// rightChild 不再使用，可以标记或放入空闲列表
	// rightChild.dirty = true // 可选，如果需要写回空状态

	// 写入修改
	if err := bt.putNode(parent); err != nil {
		return fmt.Errorf("mergeNodes 写入父节点 %d 失败: %w", parent.pageID, err)
	}
	if err := bt.putNode(leftChild); err != nil {
		return fmt.Errorf("mergeNodes 写入左子节点 %d 失败: %w", leftChild.pageID, err)
	}
	// TODO: 处理 rightChild.pageID 的回收

	// fmt.Printf("合并完成。左节点 %d 现在有 %d 个键。\n", leftChild.pageID, leftChild.numKeys)
	return nil
}

// ==========================================================================
// 主函数 (示例用法)
// ==========================================================================

func main() {
	dbFile := "my_bptree.db" // 数据库文件名

	// 清理旧文件以便从头开始测试 (可选)
	fmt.Printf("清理旧数据库文件 '%s' (如果存在)...\n", dbFile)
	_ = os.Remove(dbFile)

	fmt.Println("\n--- 创建/打开 B+Tree ---")
	// 创建或打开 B+树
	bt, err := NewBTree(dbFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建/打开 B+Tree 出错: %v\n", err)
		os.Exit(1)
	}
	// 使用 defer 确保资源最终被释放
	defer func() {
		fmt.Println("\n--- 关闭 B+Tree ---")
		if err := bt.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "关闭 B+Tree 出错: %v\n", err)
		} else {
			fmt.Println("B+Tree 已成功关闭。")
		}
	}()

	fmt.Println("\n--- 插入数据 ---")
	// 准备一些键值对进行插入
	data := map[string]string{
		"apple":      "A sweet red or green fruit",
		"banana":     "A long yellow fruit",
		"cherry":     "A small red fruit",
		"date":       "A sweet brown fruit from a palm tree",
		"elderberry": "A dark purple berry",
		"fig":        "A soft pear-shaped fruit",
		"grape":      "A small round purple or green fruit",
		"honeydew":   "A type of melon", // 插入这些键会触发节点分裂
		"kiwi":       "A small brown fuzzy fruit",
		"lemon":      "A sour yellow citrus fruit",
		"mango":      "A sweet tropical fruit",
		"nectarine":  "A type of peach with smooth skin",
		"orange":     "A round orange citrus fruit",
		"peach":      "A fuzzy fruit with a large stone",
		"quince":     "A hard, acidic pear-shaped fruit",
		"raspberry":  "A small red berry",
		"strawberry": "A sweet red fruit",
		"tangerine":  "A small orange citrus fruit",
		"ugli":       "A type of citrus fruit",
		"vanilla":    "A flavoring derived from orchids",
		"watermelon": "A large green melon",
		"xigua":      "Chinese word for watermelon",
		"yam":        "A starchy tuber",
		"zucchini":   "A type of summer squash",
		// 添加更多键确保多次分裂
		"apricot":     "A soft, velvety fruit",
		"blueberry":   "A small blue berry",
		"cantaloupe":  "A type of melon",
		"dragonfruit": "A fruit with pink skin and white flesh",
	}

	// 按键排序插入，模拟有序插入场景（可能导致频繁分裂）
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys) // 按字母顺序插入

	startTime := time.Now()
	insertCount := 0
	for _, k := range keys {
		v := data[k]
		fmt.Printf("正在插入: %s\n", k) // -> %s\n", k, v) // 值可能很长，简化输出
		err := bt.Insert([]byte(k), []byte(v))
		if err != nil {
			if errors.Is(err, ErrKeyExists) {
				fmt.Printf("警告：键 '%s' 已存在，跳过插入。\n", k)
			} else {
				fmt.Fprintf(os.Stderr, "插入键 '%s' 出错: %v\n", k, err)
				// 根据需要决定是否停止
				// return
			}
		} else {
			insertCount++
		}
	}
	insertDuration := time.Since(startTime)
	fmt.Printf("\n成功插入 %d 个键，耗时 %v\n", insertCount, insertDuration)

	// 批量插入后显式刷新 (可选, Close 也会刷新)
	fmt.Println("\n--- 手动刷新脏页到磁盘 ---")
	flushStartTime := time.Now()
	if err := bt.pager.FlushDirtyPages(); err != nil {
		fmt.Fprintf(os.Stderr, "手动刷新 Pager 出错: %v\n", err)
	} else {
		fmt.Printf("脏页刷新完成，耗时 %v\n", time.Since(flushStartTime))
	}

	fmt.Println("\n--- 搜索数据 ---")
	// 测试搜索一些键 (存在的和不存在的)
	keysToSearch := []string{"kiwi", "grape", "zucchini", "missing_key", "apple", "mango"}
	searchStartTime := time.Now()
	searchCount := 0
	foundCount := 0
	for _, k := range keysToSearch {
		fmt.Printf("正在搜索: %s\n", k)
		searchCount++
		value, err := bt.Search([]byte(k))
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				fmt.Printf("  -> 未找到\n")
			} else {
				fmt.Fprintf(os.Stderr, "  -> 搜索键 '%s' 时出错: %v\n", k, err)
			}
		} else {
			foundCount++
			fmt.Printf("  -> 找到: %s\n", string(value))
		}
	}
	searchDuration := time.Since(searchStartTime)
	fmt.Printf("\n搜索 %d 个键 (%d 个找到)，耗时 %v\n", searchCount, foundCount, searchDuration)

	fmt.Println("\n--- 重新打开数据库进行验证 ---")
	// 先关闭当前的实例
	if err := bt.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "重新打开前关闭 B+Tree 出错: %v\n", err)
		// 即使关闭出错，也尝试继续重新打开
	} else {
		fmt.Println("第一个 B+Tree 实例已关闭。")
	}

	// 重新打开同一个数据库文件
	fmt.Println("尝试重新打开数据库文件...")
	bt2, err := NewBTree(dbFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "重新打开 B+Tree '%s' 出错: %v\n", dbFile, err)
		os.Exit(1)
	}
	fmt.Println("数据库已成功重新打开。")
	// 确保第二个实例也会被关闭
	defer func() {
		fmt.Println("\n--- 关闭重新打开的 B+Tree ---")
		if err := bt2.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "关闭重新打开的 B+Tree 出错: %v\n", err)
		} else {
			fmt.Println("重新打开的 B+Tree 实例已成功关闭。")
		}
	}()

	fmt.Println("\n--- 在重新打开的数据库中搜索数据 ---")
	// 在新实例中搜索，验证持久化是否成功
	reopenSearchKey := "tangerine"
	fmt.Printf("正在搜索: %s\n", reopenSearchKey)
	value, err := bt2.Search([]byte(reopenSearchKey))
	if err != nil {
		fmt.Fprintf(os.Stderr, "  -> 重新打开后搜索键 '%s' 时出错: %v\n", reopenSearchKey, err)
	} else {
		fmt.Printf("  -> 找到: %s\n", string(value))
	}

	reopenSearchKeyMissing := "another_missing_key"
	fmt.Printf("正在搜索: %s\n", reopenSearchKeyMissing)
	_, err = bt2.Search([]byte(reopenSearchKeyMissing))
	if errors.Is(err, ErrKeyNotFound) {
		fmt.Printf("  -> 未找到 (符合预期)\n")
	} else if err != nil {
		fmt.Fprintf(os.Stderr, "  -> 重新打开后搜索键 '%s' 时出错: %v\n", reopenSearchKeyMissing, err)
	} else {
		fmt.Fprintf(os.Stderr, "  -> 错误：不应找到键 '%s'\n", reopenSearchKeyMissing)
	}

	fmt.Println("\n--- 示例执行完毕 ---")
}
