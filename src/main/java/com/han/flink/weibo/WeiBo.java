package com.han.flink.weibo;

import java.io.Serializable;

public class WeiBo implements Serializable
{
	
	private static final long serialVersionUID = 1L;

	private String uid;

	private String mid;
	
	private String time;

	private int forward_count;

	private int comment_count;

	private int like_count;

	private String content;

	public String getUid()
	{
		return uid;
	}

	public void setUid(String uid)
	{
		this.uid = uid;
	}

	public String getMid()
	{
		return mid;
	}

	public void setMid(String mid)
	{
		this.mid = mid;
	}

	public int getForward_count()
	{
		return forward_count;
	}

	public void setForward_count(int forward_count)
	{
		this.forward_count = forward_count;
	}

	public int getComment_count()
	{
		return comment_count;
	}

	public void setComment_count(int comment_count)
	{
		this.comment_count = comment_count;
	}

	public int getLike_count()
	{
		return like_count;
	}

	public void setLike_count(int like_count)
	{
		this.like_count = like_count;
	}

	public String getContent()
	{
		return content;
	}

	public void setContent(String content)
	{
		this.content = content;
	}
	
	

	public String getTime()
	{
		return time;
	}

	public void setTime(String time)
	{
		this.time = time;
	}

	@Override
	public String toString()
	{
		return "WeiBo [uid=" + uid + ", mid=" + mid + ", time=" + time + ", forward_count=" + forward_count
				+ ", comment_count=" + comment_count + ", like_count=" + like_count + ", content=" + content + "]";
	}

	

}
