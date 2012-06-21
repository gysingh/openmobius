package com.ebay.erl.mobius.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.ebay.erl.mobius.core.collection.CaseInsensitiveTreeMap;

/**
 * Provides SerDe methods.
 * 
 * <p>
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Neel Sundaresan
 */
public class SerializableUtil 
{
	public final static String serializeToBase64(byte[] binary)
		throws IOException
	{
		String result = new String (Base64.encodeBase64 (binary), "UTF-8");
		return result;
	}
	
	public final static String serializeToBase64(Serializable obj)
		throws IOException
	{
		if( obj instanceof String )
		{
			boolean isBase64 = Base64.isArrayByteBase64(((String)obj).getBytes("UTF-8"));
			if( isBase64 )
				return (String)obj;
		}
		
		ByteArrayOutputStream bos	= new ByteArrayOutputStream ();
		ObjectOutputStream oos		= null;
		try
		{
			oos = new ObjectOutputStream (bos);
			oos.writeObject (obj);
			oos.flush ();
			oos.close (); 
	
			String result = new String (Base64.encodeBase64 (bos.toByteArray ()), "UTF-8");
			return result;
		}
		catch(NotSerializableException e)
		{
			throw new IllegalArgumentException("Cannot serialize "+obj.getClass().getCanonicalName(), e);
		}
		finally
		{
			try
			{ 
				bos.close (); 
			}
			catch(Throwable e){e=null;}
			
			try
			{
				if( oos!=null) 
					oos.close (); 
			}
			catch(Throwable e){e=null;}
		}
	}
	
	public static Object deserializeFromBase64(String base64String, Configuration conf)
		throws IOException
	{	
		ObjectInputStream ois = null;
		try
		{
			byte[] objBinary = Base64.decodeBase64 (base64String.getBytes ());
			
			ois = new ObjectInputStream (new ByteArrayInputStream (objBinary));			
	
			Object object = ois.readObject ();			
			
			if (conf!=null)
			{
				if( object instanceof Configurable )
				{
					((Configurable)object).setConf(conf);
				}
				else if (object instanceof Configurable[] )
				{
					Configurable[] confArray = (Configurable[])object;
					for( Configurable aConfigurable:confArray )
					{
						aConfigurable.setConf(conf);
					}
				}
			}
	
			return object;
		}		
		catch(ClassNotFoundException e)
		{
			throw new RuntimeException(e);
		}
		finally
		{
			if( ois!=null )
			{
				try
				{
					ois.close ();
				}catch(Throwable e){}
			}	
		}
	}
	
	public static Comparable<?> deseralizeComparable(DataInput in, Configuration conf)
		throws IOException
	{
		return (Comparable<?>)deserializeFromBase64(in.readUTF(), conf);
	}
	
	public static CaseInsensitiveTreeMap deseralizeTreeMap(DataInput in)
		throws IOException
	{
		CaseInsensitiveTreeMap map = new CaseInsensitiveTreeMap();
		int keys_nbr = in.readInt();
		for( int i=0;i<keys_nbr;i++ )
		{
			map.put(in.readUTF(), in.readUTF());
		}
		return map;
	}
}
