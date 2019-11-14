package com.thoughtspot.load_utility;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelShell;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSLoadUtility {
	private String host;
	private int port;
	private String username;
	private String password;
	private Session session;
	private String command;
	private static final transient Logger LOG = LoggerFactory.getLogger(TSLoadUtility.class);
	private static TSLoadUtility instance = null;
	
	public static synchronized TSLoadUtility getInstance(String host, int port, String username, String password)
	{
		if (instance == null)
			instance = new TSLoadUtility(host, port, username, password);
		
		return instance;
	}
	
	private TSLoadUtility(String host, int port, String username, String password)
	{
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
	}
	
	private TSLoadUtility(String host, String username, String password)
	{
		this(host, 22, username, password);
	}
	
	public void connect() throws TSLoadUtilityException {
		try {
			java.util.Properties config = new java.util.Properties(); 
	    	config.put("StrictHostKeyChecking", "no");
	    	JSch jsch = new JSch();
	    	Session session=jsch.getSession(this.username, this.host, this.port);
	    	session.setPassword(password);
	    	session.setConfig(config);
	    	
	    	session.connect();
	    	this.session = session;
		} catch(JSchException e) {
			throw new TSLoadUtilityException(e.getMessage());
		}
	}
	
	public void setTSLoadProperties(String database, String table, String field_separator) {
		StringBuilder sb = new StringBuilder();
		sb.append("gzip -dc | tsload --target_database ");
		sb.append(database);
		sb.append(" --target_table ");
		sb.append(table);
		sb.append(" --field_separator ");
		sb.append("'"+field_separator+"'");
		sb.append(" --null_value '' --max_ignored_rows 10");
		this.command = sb.toString();
	}
	
	public LinkedHashMap<String,String> getTableColumns(String database, String schema, String table) throws TSLoadUtilityException {
		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		try {
Channel channel=session.openChannel("shell");
			
			PipedOutputStream pos = new PipedOutputStream();
	        PipedInputStream pis = new PipedInputStream(pos);
	        channel.setInputStream(pis);
	        pos.write(("tql\nuse "+database+";\nshow table "+schema+"."+table+";\nexit;\nexit\n").getBytes());
	        pos.flush();
	        pos.close();
	        InputStream in=channel.getInputStream();
	        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
	        channel.setOutputStream(baos);
	        channel.connect();
	        Thread.sleep(10000);
	        
	        String[] output = new String(baos.toByteArray()).replaceAll("\r", "").split("\n");
	        boolean flag = false;
	        
	        for (String line : output)
	        {
	        	if (flag && line.startsWith("Statement"))
	        	{
	        		flag = false;
	        	}
	        	if (flag)
	        	{
	        		String[] segments = line.split("\\|");
	        		columns.put(segments[0].trim(),segments[2].trim());
	        	}
	        	if (line.startsWith("--------------------|"))
	        	{
	        		flag = true;
	        	}
	        	
	        }

	        channel.disconnect();
			return columns;
		} catch(JSchException | IOException | InterruptedException e)
		{
			throw new TSLoadUtilityException(e.getMessage());
		}
	}

	public void createTable(LinkedHashMap<String, String> attributes, String table, String database) throws TSLoadUtilityException
	{
		StringBuilder command = new StringBuilder();
		command.append("tql\nuse " + database+";\ncreate table " + table + "(");
		int idx = 0;
		for (String key : attributes.keySet())
		{
			command.append(key +" " + attributes.get(key));
			if (idx++ != attributes.size() - 1)
				command.append(", ");
		}

		command.append(");\nexit;\nexit\n");
		LOG.info("TSLU:: " + command.toString());
		try {
			Channel channel=session.openChannel("shell");

			PipedOutputStream pos = new PipedOutputStream();
			PipedInputStream pis = new PipedInputStream(pos);
			channel.setInputStream(pis);
			pos.write(command.toString().getBytes());
			pos.flush();
			pos.close();
			InputStream in=channel.getInputStream();
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			channel.setOutputStream(baos);
			channel.connect();

			Thread.sleep(10000);
			String output = new String(baos.toByteArray()).replaceAll("\r", "");
			LOG.info("TSLU:: " + output);
				if (!output.contains("Statement executed successfully."))
					throw new TSLoadUtilityException(output);
			channel.disconnect();
		} catch(JSchException | IOException | InterruptedException e) {
			LOG.error("TSLU:: " + e.getMessage());
			throw new TSLoadUtilityException(e.getMessage());
		}
	}

	public ArrayList<String> getTables(String database) throws TSLoadUtilityException {
		ArrayList<String> tables = new ArrayList<String>();
		try {
			Channel channel=session.openChannel("shell");
			
			PipedOutputStream pos = new PipedOutputStream();
	        PipedInputStream pis = new PipedInputStream(pos);
	        channel.setInputStream(pis);
	        pos.write(("tql\nuse "+database+";\nshow tables;\nexit;\nexit\n").getBytes());
	        pos.flush();
	        pos.close();
	        InputStream in=channel.getInputStream();
	        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
	        channel.setOutputStream(baos);
	        channel.connect();
	        
	        Thread.sleep(10000);
	        String[] output = new String(baos.toByteArray()).replaceAll("\r", "").split("\n");
	        boolean flag = false;
	        
	        for (String line : output)
	        {
	        	if (flag && line.startsWith("Statement"))
	        	{
	        		flag = false;
	        	}
	        	if (flag)
	        	{
	        		String[] segments = line.split("\\|");
	        		tables.add(segments[0].trim()+"."+segments[1].trim());
	        	}
	        	if (line.startsWith("----"))
	        	{
	        		flag = true;
	        	}
	        	
	        }

	        channel.disconnect();
	        return tables;
		} catch(JSchException | IOException | InterruptedException e)
		{
			throw new TSLoadUtilityException(e.getMessage());
		}
	}
	
	public void loadData(List<String> records) throws TSLoadUtilityException {
		StringBuilder recs = new StringBuilder();
		for (String rec : records)
		{
			recs.append(rec+"\n");
		}
		String recsToLoad = recs.toString();
		try {
		Channel channel=session.openChannel("exec");
    	ByteArrayOutputStream byteStream = new ByteArrayOutputStream(recsToLoad.length());
    	GZIPOutputStream gos = new GZIPOutputStream(byteStream);
        gos.write(recsToLoad.getBytes());
        gos.flush();
        gos.close();
        byteStream.close();
        ((ChannelExec)channel).setCommand(this.command);
        
        
        ((ChannelExec)channel).setErrStream(System.err);
        
        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos);
        channel.setInputStream(pis);
        pos.write(byteStream.toByteArray());
        pos.flush();
        pos.close();
        InputStream in=channel.getInputStream();
        
        channel.setOutputStream(System.out);
        channel.connect();
        
        
        byte[] tmp=new byte[1024];
        while(true){
          while(in.available()>0){
            int i=in.read(tmp, 0, 1024);
            if(i<0)break;
            System.out.print(new String(tmp, 0, i));
          }
          if(channel.isClosed()){
            System.out.println("exit-status: "+channel.getExitStatus());
            break;
          }
          
          
          try{Thread.sleep(1000);}catch(Exception ee){}
        }

        channel.disconnect();
		} catch(JSchException | IOException  e) {
			throw new TSLoadUtilityException(e.getMessage());
		} 
	}

	public LinkedHashSet<String> retrieve(String database, String table) throws TSLoadUtilityException
	{
		LinkedHashSet<String> records = new LinkedHashSet<>();
		try {
			Channel channel=session.openChannel("shell");

			PipedOutputStream pos = new PipedOutputStream();
			PipedInputStream pis = new PipedInputStream(pos);
			channel.setInputStream(pis);
			pos.write(("tql\nuse "+database+";\nselect * from "+table+" limit 50;\nexit;\nexit\n").getBytes());
			pos.flush();
			pos.close();
			InputStream in=channel.getInputStream();
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			channel.setOutputStream(baos);
			channel.connect();

			Thread.sleep(10000);
			String[] output = new String(baos.toByteArray()).replaceAll("\r", "").split("\n");
			boolean flag = false;

			for (String line : output)
			{
				if (flag && line.startsWith("("))
				{
					flag = false;
				}
				if (flag)
				{
					records.add(line);
				}
				if (line.startsWith("----"))
				{
					flag = true;
				}

			}

			channel.disconnect();
			return records;
		} catch(JSchException | IOException | InterruptedException e) {
			throw new TSLoadUtilityException(e.getMessage());
		}
	}

	public void disconnect() {
        session.disconnect();
	}
}
