require "file_loader/version"
require 'shellwords'
require 'socket'
require 'fileutils'

module FileLoader
  extend self

  def parse_url(url)
    url.match(/^(?:(?<protocol>\w+):\/\/(?:(?<user>.+?)(?:\:(?<password>.+?))?@)?(?<host>.+?):?)?(?<path>\/.+)?$/) or raise("Invalid url #{url}")
  end

  def build_url(opts)
    res = ''
    res += "#{opts[:protocol]}://" if opts[:protocol]
    res += opts[:user].to_s
    res += ":#{opts[:password]}" if opts[:password]
    res += '@' if opts[:user]
    res += "#{opts[:host]}:" if opts[:host]
    res += opts[:path].to_s
  end

  def download(src, dst, opts = {})
    src = src.shellescape
    dst = dst.shellescape
    psrc = parse_url(src)
    unless Dir.exists?(File.dirname(dst))
      FileUtils.mkdir_p(File.dirname(dst), :mode => opts[:permissions] ? opts[:permissions].to_i(8) : nil)
    end
    case psrc[:protocol]
    when 'http', 'https', 'ftp'
      cmd = "wget #{src} -O #{dst}"
    when 'scp'
      if Socket.gethostname == psrc[:host]
        cmd = cp_cmd(psrc[:path], dst)
      else
        cmd = "scp -r -oBatchMode=yes -oStrictHostKeyChecking=no "
        cmd += "-l #{speed_limit} " if opts[:speed_limit]
        cmd += '"'
        cmd += build_url(user: psrc[:user] || opts[:user], password: psrc[:password] || opts[:password], host: psrc[:host], path: psrc[:path])
        cmd += '"'
        cmd += " \"#{dst}\""
      end
    when nil
      cmd = cp_cmd(src, dst)
    else
      raise "Unsupported protocol #{psrc[:protocol]}"
    end
    opts[:logger].debug(cmd) if opts[:logger]
    exec_cmd(cmd, opts[:retries].to_i) unless opts[:dry_run]
  end

  def upload(src, dst, opts = {})
    src = src.shellescape
    dst = dst.shellescape
    parsed_dst = parse_url(dst)
    case parsed_dst[:protocol]
    when 'scp'
      if Socket.gethostname == parsed_dst[:host]
        unless Dir.exists?(File.dirname(dst))
          FileUtils.mkdir_p(File.dirname(dst), :mode => opts[:permissions] ? opts[:permissions].to_i(8) : nil)
        end
        cmd = cp_cmd(src, parsed_dst[:path])
      else
        cmd = "ssh -oBatchMode=yes -oStrictHostKeyChecking=no "
        user = parsed_dst[:user] || opts[:user]
        cmd += user if user
        cmd += ":#{parsed_dst[:password]}" if parsed_dst[:password]
        cmd += '@' if user
        cmd += "#{parsed_dst[:host]} \""
        if opts[:permissions]
          umask = 'umask %03o' % ('0777'.to_i - opts[:permissions].to_i)
          cmd += "(#{umask}; "
        end
        cmd += "mkdir -p #{File.dirname(parsed_dst[:path])}"
        cmd += ")" if opts[:permissions]
        cmd += "\" && scp -r -oBatchMode=yes -oStrictHostKeyChecking=no "
        cmd += "-l #{opts[:speed_limit]} " if opts[:speed_limit]
        cmd += "#{src} \""
        cmd += user if user
        cmd += ":#{opts[:password]}" if opts[:password]
        cmd += '@' if user
        cmd += "#{parsed_dst[:host]}:#{parsed_dst[:path]}\""
      end
    when nil
      unless Dir.exists?(File.dirname(dst))
        FileUtils.mkdir_p(File.dirname(dst), :mode => opts[:permissions] ? opts[:permissions].to_i(8) : nil)
      end
      cmd = cp_cmd(src, dst)
    else
      raise "Unsupported protocol #{parsed_src[:protocol]}"
    end
    opts[:logger].debug(cmd) if opts[:logger]
    exec_cmd(cmd, opts[:retries].to_i) unless opts[:dry_run]
  end

  private

  def exec_cmd(cmd, retries = 0, delay = 5)
    return unless cmd
    cmd += " 2>&1"
    (retries + 1).times do |n|
      res = `#{cmd}`
      if $?.exitstatus == 0
        break
      elsif n >= retries
        raise res
      else
        sleep delay
      end
    end
  end

  def cp_cmd(src, dst, permissions = nil)
    return if src == dst
    "cp -r #{src} #{dst}"
  end
end
