package db;

public class DbIntergrityException  extends RuntimeException{

	private static final long serialVersionUID = 1L;
	
	public DbIntergrityException(String msg) {
		super(msg);
	}
	
}
