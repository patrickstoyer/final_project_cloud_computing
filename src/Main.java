import java.awt.*; 
import javax.swing.*;
import java.awt.event.*; // Create a simple GUI window 
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Blob;
import com.google.auth.oauth2.*;
import com.google.api.services.dataproc.*;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import java.util.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.api.client.json.jackson2.JacksonFactory;
		
public class Main {  
	private static JFrame frame; 
	private static JButton tolstoyBtn = new JButton("Tolstoy");
	private static JTextField searchText = new JTextField(100);
//	private static JTextField numberText = new JTextField(10);
	private static JButton shakespeareBtn = new JButton("Shakespeare");
	private static JButton hugoBtn = new JButton("Hugo");
 	private static JButton resultsSearchBtn = new JButton("Search terms");
	private static StringBuilder results = new StringBuilder();
	
	private static void createWindow() {       //Create and set up the window.   
		Dimension dim = Toolkit.getDefaultToolkit().getScreenSize();
    
		frame = new JFrame(); 
		frame.setLayout(null);
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);       
    tolstoyBtn.addActionListener(new ActionListener() {
    	public void actionPerformed(ActionEvent e) {	
    		createIndices("Tolstoy/");
    		secondWindow();
    	}
    });  
    shakespeareBtn.addActionListener(new ActionListener() {
    	public void actionPerformed(ActionEvent e) {	
    		createIndices("shakespeare/");
    		secondWindow();
    	}
    }); 
    hugoBtn.addActionListener(new ActionListener() {
    	public void actionPerformed(ActionEvent e) {	
    		createIndices("hugo/");
    		secondWindow();
    	}
    });

  	searchText.setBounds(0,0, (int) dim.getWidth(), 2*(int) dim.getHeight()/3);;
    tolstoyBtn.setBounds(0, 0, (int) dim.getWidth(), (int) dim.getHeight()/3);
    resultsSearchBtn.setBounds(0,2*(int)dim.getHeight()/3,(int) dim.getWidth(),(int) dim.getHeight()/3);
    shakespeareBtn.setBounds(0, (int)dim.getHeight()/3, (int) dim.getWidth(), (int) dim.getHeight()/3);
    hugoBtn.setBounds(0, 2*(int)dim.getHeight()/3, (int) dim.getWidth(), (int) dim.getHeight()/3);
    //  btn.setPreferredSize(new Dimension(300, 100));      
    frame.getContentPane().add(tolstoyBtn); 

    frame.getContentPane().add(shakespeareBtn); 
    frame.getContentPane().add(hugoBtn); //Display the window.    
    frame.setLocationRelativeTo(null);       
    frame.setVisible(true);   
    frame.setBounds(0, 0, (int) dim.getWidth(), (int) dim.getHeight());
  } 
	private static void secondWindow() {
		frame.getContentPane().removeAll();
		frame.getContentPane().add(searchText);
		frame.getContentPane().add(resultsSearchBtn); 
		resultsSearchBtn.addActionListener(new ActionListener() {
    	public void actionPerformed(ActionEvent e) {	
    		resultsSearch();
    		thirdWindow();
    	}
    });
		frame.getContentPane().revalidate();
		frame.getContentPane().repaint();
	}
	private static void thirdWindow() {
		frame.getContentPane().removeAll();
		JLabel text = new JLabel(results.toString());
		frame.getContentPane().add(text);
		frame.getContentPane().revalidate();
		frame.getContentPane().repaint();
	}
  public static void main(String[] args) throws Exception
  { 
  	createWindow();  	
  } 
  private static void createIndices(String author) {
  	String fileUri = "gs://stoyer-final-project-bucket/" + author;
    
    try {
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("./credentials.json"))
            .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
        Dataproc dp = new Dataproc.Builder(new NetHttpTransport(),
        																	 new JacksonFactory(), 
        																	 requestInitializer).build();
        
        HadoopJob hj = new HadoopJob();
        
        hj.setMainJarFileUri("gs://stoyer-final-project-bucket/jar/invertedindex.jar");
        hj.setArgs(ImmutableList.of("InvertedIndex",
                     fileUri,
                     "gs://stoyer-final-project-bucket/output"));
        
        dp.projects().regions().jobs().submit("final-project-310623" , 
	        																						"us-central1", 
	        																						new SubmitJobRequest()
	        																						.setJob(new Job()
	        																						.setPlacement(new JobPlacement()
	        																								.setClusterName("final-proj-cluster-stoyer")
        																									)
        																									.setHadoopJob(hj)
        																							)
        																						).execute();

    } catch (Exception exc) {
        exc.printStackTrace();
    }
  }
  private static void resultsSearch() {
  	String searchStr = searchText.getText();
  	System.out.println(searchStr);
  	try {
      GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("./credentials.json"))
                  .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
      Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
            
      Blob blob = storage.get(BlobId.of("stoyer-final-project-bucket", "output.txt"));
      blob.downloadTo(Paths.get("./output.txt"));
      
      Scanner input = new Scanner(new File("output.txt"));
      
      while(input.hasNextLine()) {
          String curr = input.nextLine();
          if (searchStr.equals(curr.split(" ")[0])) {
          	results.append(curr.substring(curr.indexOf(" ")));
          }
      }
  	} catch (Exception exc) {
      exc.printStackTrace();
  	}

  }
  /*private static void resultsTop() {
  	int numberOfTerms = Integer.parseInt(numberText.getText());
  	try {
      GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("./credentials.json"))
                  .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
      Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
            
      Blob blob = storage.get(BlobId.of("stoyer-final-project-bucket", "output.txt"));
      blob.downloadTo(Paths.get("./output.txt"));
      
      Scanner input = new Scanner(new File("output.txt"));
      while(input.hasNextLine()) {
          String curr = input.nextLine();
          curr.split(":");
      }
  	} catch (Exception exc) {
      exc.printStackTrace();
  	}
  	
  }*/

}

