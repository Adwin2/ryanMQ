package utils

import (
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
)

// ZeroCopyFile 使用sendfile系统调用实现零拷贝文件传输
func ZeroCopyFile(src, dst string) error {
	// 打开源文件
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	// 获取文件信息
	stat, err := srcFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	// 创建目标文件
	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer dstFile.Close()

	// 使用sendfile系统调用
	_, err = syscall.Sendfile(int(dstFile.Fd()), int(srcFile.Fd()), nil, int(stat.Size()))
	if err != nil {
		return fmt.Errorf("sendfile failed: %w", err)
	}

	return nil
}

// ZeroCopyNetworkTransfer 使用sendfile将文件直接传输到网络连接
func ZeroCopyNetworkTransfer(conn net.Conn, filePath string) error {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// 获取文件信息
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// 获取TCP连接的文件描述符
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("connection is not a TCP connection")
	}

	// 获取文件描述符
	connFile, err := tcpConn.File()
	if err != nil {
		return fmt.Errorf("failed to get connection file: %w", err)
	}
	defer connFile.Close()

	// 使用sendfile系统调用
	_, err = syscall.Sendfile(int(connFile.Fd()), int(file.Fd()), nil, int(stat.Size()))
	if err != nil {
		return fmt.Errorf("sendfile failed: %w", err)
	}

	return nil
}

// MMapRead 使用内存映射读取文件
func MMapRead(filePath string) ([]byte, error) {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// 获取文件信息
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// 进行内存映射
	mmap, err := syscall.Mmap(
		int(file.Fd()),
		0,
		int(stat.Size()),
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	// 返回映射的内存
	// 注意：调用者负责在使用完毕后调用syscall.Munmap(mmap)解除映射
	return mmap, nil
}

// ZeroCopyReader 实现io.Reader接口，使用sendfile优化
type ZeroCopyReader struct {
	file *os.File
}

// NewZeroCopyReader 创建一个新的零拷贝读取器
func NewZeroCopyReader(filePath string) (*ZeroCopyReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	return &ZeroCopyReader{file: file}, nil
}

// WriteTo 实现io.WriterTo接口，使用sendfile优化
func (r *ZeroCopyReader) WriteTo(w io.Writer) (int64, error) {
	// 尝试获取文件描述符
	if wf, ok := w.(interface {
		Fd() uintptr
	}); ok {
		// 获取文件信息
		stat, err := r.file.Stat()
		if err != nil {
			return 0, fmt.Errorf("failed to stat file: %w", err)
		}

		// 使用sendfile系统调用
		n, err := syscall.Sendfile(int(wf.Fd()), int(r.file.Fd()), nil, int(stat.Size()))
		return int64(n), err
	}

	// 回退到标准复制
	return io.Copy(w, r.file)
}

// Read 实现io.Reader接口
func (r *ZeroCopyReader) Read(p []byte) (int, error) {
	return r.file.Read(p)
}

// Close 关闭文件
func (r *ZeroCopyReader) Close() error {
	return r.file.Close()
}
