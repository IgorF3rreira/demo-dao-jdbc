package application;

import java.util.Date;
import java.util.List;

import model.dao.DaoFactory;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class Program {

	public static void main(String[] args) {
		//Department obj = new Department(1, "Books");
		
		//Seller seller = new Seller(21, "Bob ", "bob@gmail.com", new Date(), 3000.0, obj);
		
		SellerDao sellerDao =  DaoFactory.createSellerDao();
		
		System.out.println("=== teste 1: Seller findById =======");
		
		Seller seller = sellerDao.findById(3);
		System.out.println(seller);
		
		System.out.println("\n=== teste 2: Seller findById =======");
		Department department = new Department(2, null);
		List<Seller> list = sellerDao.findByDepartment(department);
		for (Seller obj : list) {
			System.out.println(obj);
		}
		
		
		System.out.println("\n=== teste 3: Seller findAll =======");
		 list = sellerDao.findAll();
		for (Seller obj : list) {
			System.out.println(obj);
		} 
		
		
		System.out.println("\n=== teste 4: Seller Insert =======");
		//O ID VAMOS COLOCAR COMO NULO POIS ELE NAO EXISTE AINDA
		Seller newSeller = new Seller(null, "Greg", "Greg@gamil.com", new Date(), 4000.0, department);
		sellerDao.insert(newSeller);
		System.out.println("Inserido novo Id = " + newSeller.getId());
		
		

	}

}
