require "file_loader/version"
require 'shellwords'
require 'socket'
require 'logger'

module FileLoader
  extend self
  
  # === Options
  # [:permissions] b.e. '0777'
  # [:speed_limit] in Kbit/s
  # [:user] default user, used if url has no user
  # [:password]
  # [:logger] Logger instance
  # [:retries] count of retries, default 1
  # [:delay] sleep seconds between retries
  # [:dry_run]
  attr_accessor :defaults
  self.defaults = {
    retries: 0,
    logger: Logger.new(STDOUT),
    delay: 5
  }

  def parse_url(url)
    url.match(/^(?:(?<protocol>\w+):\/\/(?:(?<user>.+?)(?:\:(?<password>.+?))?@)?(?<host>.+?):?)?(?<path>\/.+)?$/) or raise("Invalid url #{url}")
  end

  def build_url(opts)
    res = ''
    res += "#{opts[:protocol]}://" if opts[:protocol]
    res += opts[:user].to_s
    res += ":#{opts[:password]}" if opts[:password]
    res += '@' if opts[:user]
    res += opts[:host] if opts[:host]
    res += ':' if opts[:host] && opts[:path]
    res += opts[:path] if opts[:path]
    res
  end

  def exists?(url, opts = {})
    purl = parse_url(url)
    case purl[:protocol]
    when 'http', 'https', 'ftp'
      cmd = "curl -I \"#{url}\""
    when 'scp'
      cmd = "ssh -oBatchMode=yes -oStrictHostKeyChecking=no "
      cmd += build_url(user: purl[:user] || opts[:user], password: purl[:password] || opts[:password], host: purl[:host])
      cmd += ' "test -f '
      cmd += purl[:path]
      cmd += '"'
    when nil
      cmd = 'test -f ' + purl[:path]
    else
      return false
    end
    system(cmd + ' 2>/dev/null 1>/dev/null')
  end

  def download(src, dst, opts = {})
    pdst = parse_url(dst = dst.shellescape)
    raise "Unsupported dst protocol #{pdst[:protocol]}" if pdst[:protocol]
    
    opts = defaults.merge(opts)

    psrc = parse_url(src = src.shellescape)
    case psrc[:protocol]
    when 'http', 'https', 'ftp'
      cmd = "wget #{src} -O #{dst}"
      cmd += " --limit-rate #{opts[:speed_limit] * 1024 / 8}" if opts[:speed_limit]
    when 'scp'
      if Socket.gethostname == psrc[:host]
        cmd = cp_cmd(psrc[:path], dst)
      else
        cmd = "scp -r -oBatchMode=yes -oStrictHostKeyChecking=no "
        cmd += "-l #{opts[:speed_limit]} " if opts[:speed_limit]
        cmd += '"'
        cmd += build_url(user: psrc[:user] || opts[:user], password: psrc[:password] || opts[:password], host: psrc[:host], path: psrc[:path])
        cmd += '" "'
        cmd += dst
        cmd += '"'
      end
    when nil
      cmd = cp_cmd(src, dst)
    else
      raise "Unsupported src protocol #{psrc[:protocol]}"
    end
    cmd = mkdir_cmd(dst, opts) + ' && ' + cmd if cmd
    exec_cmd(cmd, opts)
  end

  def upload(src, dst, opts = {})
    psrc = parse_url(src = src.shellescape)
    raise "Unsupported src protocol #{psrc[:protocol]}" if psrc[:protocol]
  
    opts = defaults.merge(opts)
      
    pdst = parse_url(dst = dst.shellescape)
    case pdst[:protocol]
    when 'scp'
      if Socket.gethostname == pdst[:host]
        cmd = cp_cmd(src, pdst[:path])
        cmd = mkdir_cmd(pdst[:path], opts) + ' && ' + cmd if cmd
      else
        cmd = "ssh -oBatchMode=yes -oStrictHostKeyChecking=no "
        cmd += build_url(user: pdst[:user] || opts[:user], password: pdst[:password], host: pdst[:host])
        cmd += ' "'
        cmd += mkdir_cmd(pdst[:path], opts)
        cmd += '" && scp -r -oBatchMode=yes -oStrictHostKeyChecking=no '
        cmd += "-l #{opts[:speed_limit]} " if opts[:speed_limit]
        cmd += "#{src} \""
        cmd += build_url(user: pdst[:user] || opts[:user], password: pdst[:password] || opts[:password], host: pdst[:host], path: pdst[:path])
        cmd += '"'
      end
    when nil
      cmd = cp_cmd(src, dst)
      cmd = mkdir_cmd(pdst[:path], opts) + ' && ' + cmd if cmd
    else
      raise "Unsupported protocol #{parsed_src[:protocol]}"
    end
    exec_cmd(cmd, opts)
  end

  private

  def exec_cmd(cmd, opts)
    opts[:logger].debug(cmd) if opts[:logger]
    return if opts[:dry_run] || !cmd
    cmd += " 2>&1"
    (opts[:retries].to_i + 1).times do |n|
      res = `#{cmd}`
      if $?.exitstatus == 0
        break
      elsif n >= opts[:retries].to_i
        raise res
      else
        opts[:logger].debug('retry') if opts[:logger]
        sleep(opts[:delay] || 5)
      end
    end
  end

  def cp_cmd(src, dst)
    return if src == dst
    "cp -r #{src} #{dst}"
  end

  def mkdir_cmd(dst, opts)
    cmd = "mkdir -p #{File.dirname(dst)}"
    if opts[:permissions]
      umask = 'umask %03o' % ('0777'.to_i - opts[:permissions].to_i)
      cmd = "(#{umask}; #{cmd})"
    end
    cmd
  end
end