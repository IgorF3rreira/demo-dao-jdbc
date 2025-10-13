package model.dao.impl;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {
	
	//A CONEXÃO SERA AQUI, ONDE TODAS AS FUNCÇÕES TERA QUE APENAS CHAMAR ELA PARA PODER TER ACESSO AO BANCO DE DADOS
	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}
	

	@Override
	public void insert(Seller obj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void update(Seller obj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteById(Integer id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Seller findById(Integer id) {
		
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
				    "SELECT seller.*, department.Name as DepName "
				    	    + "FROM seller INNER JOIN department "
				    	    + "ON seller.DepartmentId = department.Id "
				    	    + "WHERE seller.Id = ?");
			
			st.setInt(1, id);
			rs = st.executeQuery();
			//VERIFICAÇÃO PARA SABER SE ENCONTROU UM VENDEDOR PARA ASSIM PODER TRANSFORMAR A INFOs BUSCADAS EM ORIENTADO A OBJETOS E NAO EM FORMA DE TABELA COMO O RESULTSET TRAAS
			if(rs.next()) {
				
				//PARA PODER PEGAR O ID
				Department dep = new Department();
				//PEGAR OS DADOS DO DEPARTMENT
				dep.setId(rs.getInt("DepartmentId"));
				dep.setName(rs.getString("DepName"));
				
				//PEGAR OS DADOS DO SELLER
				Seller obj = new Seller();
				obj.setId(rs.getInt("Id"));
				obj.setName(rs.getString("Name"));
				obj.setEmail(rs.getString("Email"));
				obj.setBaseSalary(rs.getDouble("BaseSalary"));
				obj.setBirthDate(rs.getDate("BirthDate"));
				//no caso do departamento vamos querer o objeto montado q no caso é o dep que criamos
				obj.setDepartment(dep);
				return obj;
				
				
			}
			return null;
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
			//NAO VAMOS FECHAR CONEXAO PQ ESSA PAGINA TEM OUTRAS FUNCOES QUE VAO PRECISAR DA CONEXAO ATIVA
		}
		
		
	}

	@Override
	public List<Seller> findAll() {
		// TODO Auto-generated method stub
		return null;
	}

}
