package mediainspect

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os/exec"
	"os"
	"runtime"
	"path/filepath"
	"testing"
	"time"
	"sync/atomic"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/source"
)

type statOnly struct{ meta source.Metadata }
func (s statOnly) Stat(context.Context,string)(source.Metadata,error){return s.meta,nil}
func (s statOnly) OpenRange(context.Context,source.RangeRequest)(io.ReadCloser,source.RangeReply,error){
	return nil,source.RangeReply{},source.ErrNotConfigured
}
type testRegistry struct{ meta source.Metadata }
func (r testRegistry) ForNode(_ context.Context,node string)(source.Client,error){
	if node!=r.meta.OwnerNodeID{return nil,source.ErrSourceOffline}
	return statOnly{meta:r.meta},nil
}
type rangedFactory struct{ data []byte }
func (f rangedFactory) Open(_ context.Context,_ source.Decision,offset int64)(source.Reader,error){
	if offset<0||offset>int64(len(f.data)){return nil,source.ErrInvalidReference}
	return io.NopCloser(bytes.NewReader(f.data[offset:])),nil
}
func setup(t *testing.T,data []byte,mime string)(source.Token,*source.Plans,map[source.Strategy]source.ReaderFactory){
	t.Helper()
	sz:=int64(len(data))
	meta:=source.Metadata{OwnerNodeID:"storage-1",FileID:"file-1",
		Version:"stable-version",SizeBytes:&sz,ContentType:mime,RangeReadable:true}
	plans:=source.NewPlans()
	token:=source.Token{RoomID:"room-1",PlaybackID:"play-1",Generation:1}
	co:=source.Coordinator{Resolver:&source.Resolver{Registry:testRegistry{meta}},Plans:plans}
	if _,err:=co.Prepare(context.Background(),token,source.Reference{StorageNodeID:"storage-1",FileID:"file-1"});err!=nil{
		t.Fatal(err)
	}
	readers:=map[source.Strategy]source.ReaderFactory{
		source.RemotePrefetch:rangedFactory{data:data},source.RemoteBlockStream:rangedFactory{data:data},
	}
	return token,plans,readers
}
func TestKAN61SeekableVersionFencedAndBounded(t *testing.T){
	payload:=[]byte("abcdefghij")
	token,plans,readers:=setup(t,payload,"audio/mpeg")
	budget:=&atomic.Int64{};budget.Store(5)
	r:=&seekable{ctx:context.Background(),lifetime:context.Background(),
		token:token,plans:plans,readers:readers,size:int64(len(payload)),budget:budget}
	defer r.Close()
	if pos,err:=r.Seek(2,io.SeekStart);err!=nil||pos!=2{t.Fatal(pos,err)}
	out:=make([]byte,3)
	if n,err:=r.Read(out);n!=3||err!=nil||string(out)!="cde"{t.Fatal(n,err,string(out))}
	if pos,err:=r.Seek(-2,io.SeekEnd);err!=nil||pos!=8{t.Fatal(pos,err)}
	if n,err:=r.Read(out);n!=2||err==nil&&n==0||string(out[:n])!="ij"{t.Fatal(n,err)}
	if n,err:=r.Read(out);n!=0||err!=io.EOF{t.Fatal(n,err)}
	if budget.Load()!=0{t.Fatalf("byte budget escaped: %d",budget.Load())}
	if _,err:=r.Seek(500,io.SeekStart);err==nil{t.Fatal("invalid seek allowed")}
	if _,err:=r.Seek(-1,io.SeekStart);err==nil{t.Fatal("negative seek allowed")}
	// A new generation invalidates the old seekable, including repeated offsets.
	next:=source.Token{RoomID:token.RoomID,PlaybackID:"play-2",Generation:2}
	co:=source.Coordinator{Resolver:&source.Resolver{Registry:testRegistry{meta:source.Metadata{
		OwnerNodeID:"storage-1",FileID:"file-1",Version:"stable-version",
		SizeBytes:ptrSize(int64(len(payload))),ContentType:"audio/mpeg",RangeReadable:true,
	}}},Plans:plans}
	if _,err:=co.Prepare(context.Background(),next,source.Reference{StorageNodeID:"storage-1",FileID:"file-1"});err!=nil{t.Fatal(err)}
	r.budget.Store(10)
	if _,err:=r.Seek(0,io.SeekStart);err!=nil{t.Fatal(err)}
	if _,err:=r.Read(out);err!=source.ErrStalePlan{t.Fatalf("old playback read after switch: %v",err)}
}
func ptrSize(v int64)*int64{return &v}

// A tiny generated RIFF/WAVE file is the entire input. No fixtures, network
// services, full media files, or ffmpeg encoder are required by this test.
func tinyWAV()[]byte{
	const samples=8000
	p:=make([]byte,44+samples*2)
	copy(p[0:4],"RIFF")
	binary.LittleEndian.PutUint32(p[4:8],uint32(len(p)-8))
	copy(p[8:12],"WAVE")
	copy(p[12:16],"fmt ")
	binary.LittleEndian.PutUint32(p[16:20],16)
	binary.LittleEndian.PutUint16(p[20:22],1)
	binary.LittleEndian.PutUint16(p[22:24],1)
	binary.LittleEndian.PutUint32(p[24:28],8000)
	binary.LittleEndian.PutUint32(p[28:32],16000)
	binary.LittleEndian.PutUint16(p[32:34],2)
	binary.LittleEndian.PutUint16(p[34:36],16)
	copy(p[36:40],"data")
	binary.LittleEndian.PutUint32(p[40:44],samples*2)
	return p
}
func TestKAN61FFprobeRealLoopbackRange(t *testing.T){
	if _,err:=exec.LookPath("ffprobe");err!=nil{t.Skip("ffprobe not installed on CI runner")}
	token,plans,readers:=setup(t,tinyWAV(),"audio/wav")
	inspector:=New("ffprobe",plans,readers)
	ctx,cancel:=context.WithTimeout(context.Background(),12*time.Second)
	defer cancel()
	media,err:=inspector.Probe(ctx,token)
	if err!=nil{t.Fatalf("trusted loopback ffprobe failed: %v",err)}
	if media.Container!="wav"||media.DurationMS<900||media.DurationMS>1100||
		len(media.Tracks)!=1||media.Tracks[0].Codec!="pcm_s16le"{
		t.Fatalf("unexpected RIFF result: %+v",media)
	}
}

func TestKAN61ProbeTimeoutKillsSubprocess(t *testing.T){
	if runtime.GOOS=="windows"{t.Skip("POSIX shell fixture")}
	token,plans,readers:=setup(t,tinyWAV(),"audio/wav")
	executable:=filepath.Join(t.TempDir(),"slow-ffprobe")
	if err:=os.WriteFile(executable,[]byte("#!/bin/sh\nsleep 5\n"),0700);err!=nil{t.Fatal(err)}
	p:=New(executable,plans,readers)
	p.Timeout=70*time.Millisecond
	start:=time.Now()
	_,err:=p.Probe(context.Background(),token)
	if err==nil{t.Fatal("hung ffprobe was accepted")}
	if time.Since(start)>2*time.Second{t.Fatalf("timeout not enforced: %v",time.Since(start))}
}

func TestKAN61ProbeCacheCannotBypassSourcePlan(t *testing.T){
	if _,err:=exec.LookPath("ffprobe");err!=nil{t.Skip("requires ffprobe")}
	token,plans,readers:=setup(t,tinyWAV(),"audio/wav")
	p:=New("ffprobe",plans,readers)
	first,err:=p.Probe(context.Background(),token)
	if err!=nil{t.Fatal(err)}
	// Exact same room, playback, generation and source version may use cache.
	p.Binary="absent-ffprobe-binary-cache-test"
	again,err:=p.Probe(context.Background(),token)
	if err!=nil||first.DurationMS!=again.DurationMS{t.Fatalf("cache was not used: %v",err)}
	// STOP removes the authoritative source plan; cached metadata is no
	// authorization to continue probing that room.
	plans.Remove(token.RoomID)
	if _,err=p.Probe(context.Background(),token);err!=source.ErrStalePlan{
		t.Fatalf("cache bypassed STOP: %v",err)
	}
}
