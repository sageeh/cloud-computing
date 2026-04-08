for i in {1..5000}; do 
  curl -s -X POST http://localhost:8080/orders \
    -H "Content-Type: application/json" \
    -d "{\"id\": \"order-$RANDOM-$i\", \"item\": \"Shampoo\", \"quantity\": 1}" > /dev/null & 
done

wait
echo "STRESS TEST COMPLETE!"