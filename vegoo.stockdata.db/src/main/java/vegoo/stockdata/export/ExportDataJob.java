package vegoo.stockdata.export;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import vegoo.stockdata.core.BaseJob;



public abstract class ExportDataJob extends BaseJob {

	
	
	public static File getFile(String filename, String relativePath) {
		String userhome = System.getProperty("user.home");
		
		File path = new File(userhome, relativePath);
		
		if(!path.exists()) {
			path.mkdirs();
		}
		
		String fileNameExt = String.format("%s.txt", filename);  // %s-%s.txt  dateTag, dataType
		
		return new File(path, fileNameExt);
	}
	
	
	protected static void writeFile(List<String> items, File file) throws IOException {
		FileWriter fw = new FileWriter(file);
		try {
			for (String s : items) {
				fw.write(s+"\n");
			}
		}finally {
		    fw.close();
		}
	}
	

}
