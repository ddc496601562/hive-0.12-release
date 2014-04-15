/**
 * This application that requires the following additional files:
 *   TreeDemoHelp.html
 *    arnold.html
 *    bloch.html
 *    chan.html
 *    jls.html
 *    swingtutorial.html
 *    tutorial.html
 *    tutorialcont.html
 *    vm.html
 */
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.UIManager;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeSelectionModel;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.baidu.rigel.hive.parse.CommondDemo;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.awt.Dimension;
import java.awt.GridLayout;

public class OpetatorTreeDemo extends JPanel
                      implements TreeSelectionListener {
    private JEditorPane htmlPane;
    private JTree tree;
    private URL helpURL;
    private static boolean DEBUG = false;

    //Optionally play with line styles.  Possible values are
    //"Angled" (the default), "Horizontal", and "None".
    private static boolean playWithLineStyle = false;
    private static String lineStyle = "Horizontal";
    
    //Optionally set the look and feel.
    private static boolean useSystemLookAndFeel = false;

    public OpetatorTreeDemo() {
        super(new GridLayout(1,0));
        
        DefaultMutableTreeNode top =null ;
        
		try {
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			System.out.println( classLoader.getResource("hive-default.xml"));
			System.out.println( classLoader.getResource("hive-site.xml"));
			HiveConf hiveConf = new HiveConf(SessionState.class);
			SessionState.start(new SessionState(hiveConf));
			Context ctx = new Context(hiveConf);
			ctx.setTryCount(10);
			ctx.setCmd(CommondDemo.command3);
			ctx.setHDFSCleanup(true);
			ParseDriver pd = new ParseDriver();
			ASTNode astTree = pd.parse(CommondDemo.command3, ctx);
			astTree = ParseUtils.findRootNonNullToken(astTree);
			SemanticAnalyzer sem =(SemanticAnalyzer)SemanticAnalyzerFactory.get(hiveConf, astTree);
			sem.analyze(astTree, ctx);
			sem.validate();		
			List<Operator<? extends OperatorDesc>>  topOpList=new ArrayList<Operator<? extends OperatorDesc>>(sem.topOps.values());
			if(topOpList.size()==1){
				top=createNodes(topOpList.get(0));
			}else {
				top=new DefaultMutableTreeNode("root");
				for(Operator<? extends OperatorDesc> op:topOpList){
					top.add(createNodes(op));
				}
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

        //Create a tree that allows one selection at a time.
        tree = new JTree(top);
        tree.getSelectionModel().setSelectionMode
                (TreeSelectionModel.SINGLE_TREE_SELECTION);

        //Listen for when the selection changes.
        tree.addTreeSelectionListener(this);

        if (playWithLineStyle) {
            System.out.println("line style = " + lineStyle);
            tree.putClientProperty("JTree.lineStyle", lineStyle);
        }

        //Create the scroll pane and add the tree to it. 
        JScrollPane treeView = new JScrollPane(tree);

        //Create the HTML viewing pane.
        htmlPane = new JEditorPane();
        htmlPane.setEditable(false);
        initHelp();
        JScrollPane htmlView = new JScrollPane(htmlPane);

        //Add the scroll panes to a split pane.
        JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        splitPane.setTopComponent(treeView);
        splitPane.setBottomComponent(htmlView);

        Dimension minimumSize = new Dimension(100, 50);
        htmlView.setMinimumSize(minimumSize);
        treeView.setMinimumSize(minimumSize);
        splitPane.setDividerLocation(100); 
        splitPane.setPreferredSize(new Dimension(500, 300));

        //Add the split pane to this panel.
        add(splitPane);
    }

    /** Required by TreeSelectionListener interface. */
    public void valueChanged(TreeSelectionEvent e) {
        DefaultMutableTreeNode node = (DefaultMutableTreeNode)
                           tree.getLastSelectedPathComponent();

        if (node == null) return;

        Object nodeInfo = node.getUserObject();
        if (node.isLeaf()) {
            BookInfo book = (BookInfo)nodeInfo;
            displayURL(book.bookURL);
            if (DEBUG) {
                System.out.print(book.bookURL + ":  \n    ");
            }
        } else {
            displayURL(helpURL); 
        }
        if (DEBUG) {
            System.out.println(nodeInfo.toString());
        }
    }

    private class BookInfo {
        public String bookName;
        public URL bookURL;

        public BookInfo(String book, String filename) {
            bookName = book;
            bookURL = getClass().getResource(filename);
            if (bookURL == null) {
                System.err.println("Couldn't find file: "
                                   + filename);
            }
        }

        public String toString() {
            return bookName;
        }
    }

    private void initHelp() {
        String s = "TreeDemoHelp.html";
        helpURL = getClass().getResource(s);
        if (helpURL == null) {
            System.err.println("Couldn't open help file: " + s);
        } else if (DEBUG) {
            System.out.println("Help URL is " + helpURL);
        }
        displayURL(helpURL);
    }

    private void displayURL(URL url) {
        try {
            if (url != null) {
                htmlPane.setPage(url);
            } else { //null url
		htmlPane.setText("File Not Found");
                if (DEBUG) {
                    System.out.println("Attempted to display a null URL.");
                }
            }
        } catch (IOException e) {
            System.err.println("Attempted to read a bad URL: " + url);
        }
    }
//    private  void getRootOperator(Collection<Operator<? extends OperatorDesc>> leafs ,Set<Operator<? extends OperatorDesc>> roots){
//    	for(Operator<? extends OperatorDesc> op :leafs){
//    		if(op.getParentOperators()!=null&&!op.getParentOperators().isEmpty())
//    			getRootOperator(op.getParentOperators(),roots);
//    		else
//    			roots.add(op);
//    	}
//    }
    private DefaultMutableTreeNode createNodes( Operator<? extends OperatorDesc> tree){
    	if(tree==null)
    		return null;
    	DefaultMutableTreeNode thisNode=new DefaultMutableTreeNode(tree.toString()+":"+tree.getConf().getClass().getSimpleName());
    	List<Operator<? extends OperatorDesc>> childer=tree.getChildOperators();
    	if(childer==null)
    		return thisNode ;
    	for(Operator<? extends OperatorDesc> child:childer){
    		DefaultMutableTreeNode childNode=createNodes(child);
    		if(childNode!=null)
    			thisNode.add(childNode);
    	}
    	return thisNode ;
    }
    /**
     * Create the GUI and show it.  For thread safety,
     * this method should be invoked from the
     * event dispatch thread.
     */
    private static void createAndShowGUI() {
        if (useSystemLookAndFeel) {
            try {
                UIManager.setLookAndFeel(
                    UIManager.getSystemLookAndFeelClassName());
            } catch (Exception e) {
                System.err.println("Couldn't use system look and feel.");
            }
        }

        //Create and set up the window.
        JFrame frame = new JFrame("TreeDemo");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        //Add content to the window.
        frame.add(new OpetatorTreeDemo());

        //Display the window.
        frame.pack();
        frame.setVisible(true);
    }

    public static void main(String[] args) {
        //Schedule a job for the event dispatch thread:
        //creating and showing this application's GUI.
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                createAndShowGUI();
            }
        });
    }
}