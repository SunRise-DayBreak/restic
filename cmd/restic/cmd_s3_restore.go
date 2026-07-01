package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	restoreui "github.com/restic/restic/internal/ui/restore"
)

// S3RestoreOptions 收集 s3restore 命令的所有选项
type S3RestoreOptions struct {
	data.SnapshotFilter

	// S3 目标配置
	S3Target    string // S3 目标地址，格式：s3:http://endpoint/bucket/prefix
	S3AccessKey string // S3 访问密钥 ID
	S3SecretKey string // S3 访问密钥 Secret
	S3Region    string // S3 区域
}

func newS3RestoreCommand(globalOptions *global.Options) *cobra.Command {
	var opts S3RestoreOptions

	cmd := &cobra.Command{
		Use:   "s3restore [flags] snapshotID",
		Short: "直接从快照流式恢复数据到 S3 对象存储",
		Long: `
"s3restore" 命令从仓库快照中读取文件内容，直接流式写入 S3 兼容对象存储，
无需经过本地临时目录。

示例：
  restic s3restore latest \
    --s3-target "s3:http://minio:9000/backup-bucket/restore/" \
    --s3-access-key minioadmin \
    --s3-secret-key minioadmin
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			finalizeSnapshotFilter(&opts.SnapshotFilter)
			return runS3Restore(cmd.Context(), opts, *globalOptions, globalOptions.Term, args)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// AddFlags 注册 s3restore 命令的参数
func (opts *S3RestoreOptions) AddFlags(f *pflag.FlagSet) {
	// S3 目标参数
	f.StringVar(&opts.S3Target, "s3-target", "", "S3 目标地址（格式：s3:http://endpoint/bucket/prefix）（必填）")
	f.StringVar(&opts.S3AccessKey, "s3-access-key", "", "S3 访问密钥（也可通过 AWS_ACCESS_KEY_ID 环境变量设置）")
	f.StringVar(&opts.S3SecretKey, "s3-secret-key", "", "S3 密钥（也可通过 AWS_SECRET_ACCESS_KEY 环境变量设置）")
	f.StringVar(&opts.S3Region, "s3-region", "", "S3 区域")

	// 快照选择参数
	initSingleSnapshotFilter(f, &opts.SnapshotFilter)
}

// s3TargetConfig 存储解析后的 S3 目标连接配置
type s3TargetConfig struct {
	endpoint  string
	bucket    string
	prefix    string
	useHTTP   bool
	accessKey string
	secretKey string
	region    string
}

// parseS3RestoreTarget 解析 S3 目标地址，返回连接配置
// 格式：s3:http://endpoint/bucket/prefix 或 s3:https://endpoint/bucket/prefix
func parseS3RestoreTarget(s3Target, accessKey, secretKey, region string) (*s3TargetConfig, error) {
	if !strings.HasPrefix(s3Target, "s3:") {
		return nil, fmt.Errorf("S3 目标地址必须以 s3: 开头")
	}

	spec := s3Target[3:]

	useHTTP := false
	if strings.HasPrefix(spec, "http://") {
		useHTTP = true
		spec = spec[7:]
	} else if strings.HasPrefix(spec, "https://") {
		spec = spec[8:]
	} else {
		return nil, fmt.Errorf("S3 目标地址必须指定 http:// 或 https:// 协议")
	}

	endpoint, rest, _ := strings.Cut(spec, "/")
	if endpoint == "" {
		return nil, fmt.Errorf("S3 目标地址缺少 endpoint")
	}

	bucket, prefix, _ := strings.Cut(rest, "/")
	if bucket == "" {
		return nil, fmt.Errorf("S3 目标地址缺少 bucket 名称")
	}

	// 环境变量回退
	if accessKey == "" {
		accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if secretKey == "" {
		secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}

	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("S3 凭证未设置，请通过 --s3-access-key/--s3-secret-key 或环境变量提供")
	}

	return &s3TargetConfig{
		endpoint:  endpoint,
		bucket:    bucket,
		prefix:    prefix,
		useHTTP:   useHTTP,
		accessKey: accessKey,
		secretKey: secretKey,
		region:    region,
	}, nil
}

// newS3Client 创建 S3 客户端
func newS3Client(cfg *s3TargetConfig) (*minio.Client, error) {
	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.Static{
			Value: credentials.Value{
				AccessKeyID:     cfg.accessKey,
				SecretAccessKey: cfg.secretKey,
			},
		},
		&credentials.EnvAWS{},
		&credentials.EnvMinio{},
	})

	return minio.New(cfg.endpoint, &minio.Options{
		Creds:  creds,
		Secure: !cfg.useHTTP,
		Region: cfg.region,
	})
}

// runS3Restore 执行 S3 流式恢复
func runS3Restore(ctx context.Context, opts S3RestoreOptions, gopts global.Options, term ui.Terminal, args []string) error {
	// 参数校验
	if opts.S3Target == "" {
		return errors.Fatal("请指定 S3 目标地址 (--s3-target)")
	}
	if len(args) == 0 {
		return errors.Fatal("请指定快照 ID")
	}
	if len(args) > 1 {
		return errors.Fatalf("只能指定一个快照 ID: %v", args)
	}

	// 解析 S3 目标配置
	s3cfg, err := parseS3RestoreTarget(opts.S3Target, opts.S3AccessKey, opts.S3SecretKey, opts.S3Region)
	if err != nil {
		return errors.Fatalf("S3 目标配置错误: %v", err)
	}

	// 创建 S3 客户端
	client, err := newS3Client(s3cfg)
	if err != nil {
		return errors.Fatalf("创建 S3 客户端失败: %v", err)
	}

	// 确保前缀以 "/" 结尾
	prefix := s3cfg.prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// 打开仓库
	var printer restoreui.ProgressPrinter
	if gopts.JSON {
		printer = restoreui.NewJSONProgress(term, gopts.Verbosity)
	} else {
		printer = restoreui.NewTextProgress(term, gopts.Verbosity)
	}

	ctx, repo, unlock, err := openWithReadLock(ctx, gopts, gopts.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	// 查找快照
	snapshotIDString := args[0]
	sn, subfolder, err := opts.SnapshotFilter.FindLatest(ctx, repo, repo, snapshotIDString)
	if err != nil {
		return errors.Fatalf("查找快照失败: %v", err)
	}

	// 加载索引
	err = repo.LoadIndex(ctx, printer)
	if err != nil {
		return err
	}

	// 定位子目录
	sn.Tree, err = data.FindTreeDirectory(ctx, repo, sn.Tree, subfolder)
	if err != nil {
		return err
	}

	if !gopts.JSON {
		fmt.Fprintf(os.Stderr, "从快照 %s 恢复到 %s\n", sn.ID().Str(), opts.S3Target)
	}

	// 遍历快照树，流式上传文件到 S3
	stats := &s3RestoreStats{}
	err = walkTreeAndUpload(ctx, repo, client, s3cfg.bucket, prefix, *sn.Tree, "", stats, gopts.JSON)
	if err != nil {
		return err
	}

	if !gopts.JSON {
		fmt.Fprintf(os.Stderr, "恢复完成: %d 个文件, %d 个目录, 总大小 %d 字节\n",
			stats.fileCount, stats.dirCount, stats.totalBytes)
	}

	return nil
}

// s3RestoreStats 恢复统计信息
type s3RestoreStats struct {
	fileCount  int
	dirCount   int
	totalBytes int64
}

// walkTreeAndUpload 递归遍历快照树，将文件流式上传到 S3
func walkTreeAndUpload(ctx context.Context, repo restic.Repository, client *minio.Client,
	bucket, prefix string, treeID restic.ID, relPath string, stats *s3RestoreStats, jsonOutput bool) error {

	// 加载当前层级的树节点
	tree, err := data.LoadTree(ctx, repo, treeID)
	if err != nil {
		return fmt.Errorf("加载目录树失败: %w", err)
	}

	for item := range tree {
		if item.Error != nil {
			return item.Error
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		node := item.Node
		nodePath := path.Join(relPath, node.Name)

		switch node.Type {
		case data.NodeTypeDir:
			// 递归处理子目录
			if node.Subtree == nil {
				return fmt.Errorf("目录 %s 缺少子树引用", nodePath)
			}
			stats.dirCount++
			if !jsonOutput {
				fmt.Fprintf(os.Stderr, "目录: %s/\n", nodePath)
			}
			err := walkTreeAndUpload(ctx, repo, client, bucket, prefix, *node.Subtree, nodePath, stats, jsonOutput)
			if err != nil {
				return err
			}

		case data.NodeTypeFile:
			// 流式上传文件到 S3
			s3Key := prefix + nodePath
			size := int64(node.Size)

			if !jsonOutput {
				fmt.Fprintf(os.Stderr, "上传: %s (%d bytes)\n", nodePath, size)
			}

			err := streamFileToS3(ctx, repo, client, bucket, s3Key, node.Content, size)
			if err != nil {
				return fmt.Errorf("上传文件 %s 失败: %w", nodePath, err)
			}
			stats.fileCount++
			stats.totalBytes += size

		default:
			// 跳过非文件非目录类型（符号链接、设备文件等）
			debug.Log("跳过不支持的节点类型: %s (%s)", nodePath, node.Type)
		}
	}

	return nil
}

// fileReader 实现 io.Reader 接口，从仓库中逐 blob 读取文件内容
// 用于将 restic 数据 blob 流式传输到 S3 PutObject
type fileReader struct {
	ctx      context.Context
	repo     restic.Repository
	content  restic.IDs // 文件的内容 blob ID 列表
	buf      []byte     // 当前 blob 的数据缓冲
	pos      int        // 当前 blob 在 content 中的索引
	offset   int        // 当前 blob 内的读取偏移
	totalLen int64      // 文件总大小（用于进度跟踪）
	readLen  int64      // 已读取的总字节数
}

// newFileReader 创建一个从仓库流式读取文件内容的 reader
func newFileReader(ctx context.Context, repo restic.Repository, content restic.IDs, totalLen int64) *fileReader {
	return &fileReader{
		ctx:      ctx,
		repo:     repo,
		content:  content,
		totalLen: totalLen,
	}
}

// Read 实现 io.Reader 接口，逐个 blob 从仓库加载数据并返回
func (r *fileReader) Read(p []byte) (int, error) {
	for {
		// 如果当前 blob 还有数据可读
		if r.buf != nil && r.offset < len(r.buf) {
			n := copy(p, r.buf[r.offset:])
			r.offset += n
			r.readLen += int64(n)
			// 当前 blob 读完，释放缓冲
			if r.offset >= len(r.buf) {
				r.buf = nil
				r.offset = 0
			}
			return n, nil
		}

		// 所有 blob 已读完
		if r.pos >= len(r.content) {
			return 0, io.EOF
		}

		// 加载下一个 blob
		blobID := r.content[r.pos]
		r.pos++
		bh := restic.BlobHandle{ID: blobID, Type: restic.DataBlob}
		buf, err := r.repo.LoadBlob(r.ctx, bh, nil)
		if err != nil {
			return 0, fmt.Errorf("加载 blob %s 失败: %w", blobID.Str(), err)
		}
		r.buf = buf
		r.offset = 0
	}
}

// streamFileToS3 将文件内容从仓库直接流式传输到 S3
// 使用 io.Pipe 实现零临时文件：reader 端传给 PutObject，writer 端由 goroutine 写入 blob 数据
func streamFileToS3(ctx context.Context, repo restic.Repository, client *minio.Client,
	bucket, key string, content restic.IDs, size int64) error {

	// 空文件直接上传
	if len(content) == 0 {
		_, err := client.PutObject(ctx, bucket, key, nil, 0,
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		return err
	}

	// 使用 Pipe 实现流式传输
	pr, pw := io.Pipe()

	// writer goroutine：从仓库加载 blob 并写入 pipe
	go func() {
		defer func() { _ = pw.Close() }()
		reader := newFileReader(ctx, repo, content, size)
		_, err := io.Copy(pw, reader)
		if err != nil {
			_ = pw.CloseWithError(err)
		}
	}()

	// PutObject 从 reader 端读取数据并上传到 S3
	// minio-go 会自动根据大小选择单次上传或分片上传
	_, err := client.PutObject(ctx, bucket, key, pr, size,
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		_ = pr.CloseWithError(err)
		return err
	}

	return nil
}
